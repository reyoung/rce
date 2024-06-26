package process

import (
	"context"
	"errors"
	"fmt"
	"github.com/creack/pty"
	"github.com/google/uuid"
	"github.com/reyoung/rce/protocol"
	"io"
	"log"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type runningState struct {
	Cmd        *exec.Cmd
	OutputChan chan *stateOutput
	Stdin      io.WriteCloser
	Stdout     io.ReadCloser
	Stderr     io.ReadCloser
	ID         string
	Complete   sync.WaitGroup
}

func (s *runningState) PID() string {
	return s.ID
}

func (s *runningState) Kill() error {
	log.Printf("Killing process")
	p := s.Cmd.Process
	if p == nil {
		log.Printf("process not started")
		return fmt.Errorf("process not started")
	}

	err := syscall.Kill(-p.Pid, syscall.SIGKILL)
	if err != nil {
		return fmt.Errorf("failed to kill process: %w", err)
	}
	return err
}

func (s *runningState) ProcessEvent(ctx context.Context, event *protocol.SpawnRequest) (newState state, err error) {
	switch event.Payload.(type) {
	case *protocol.SpawnRequest_Stdin_:
		err = s.processStdin(event.GetStdin())
		if err != nil {
			return nil, fmt.Errorf("failed to process stdin event: %w", err)
		}
		return nil, nil
	default:
		return nil, fmt.Errorf("%w: %T", errStateUnexpectedEvent, event.Payload)
	}
}

func (s *runningState) processStdin(stdin *protocol.SpawnRequest_Stdin) error {
	if s.Stdin == nil {
		return errors.New("stdin not available")
	}
	if len(stdin.Stdin) != 0 {
		_, err := s.Stdin.Write(stdin.Stdin)
		if err != nil {
			return fmt.Errorf("failed to write to stdin: %w", err)
		}
	}

	if stdin.Eof {
		err := s.Stdin.Close()
		if err != nil {
			return fmt.Errorf("failed to close stdin: %w", err)
		}
		log.Printf("stdin closed")
		s.Stdin = nil
	}
	return nil
}

func (s *runningState) Close() error {
	var res error
	if p := s.Cmd.Process; p != nil {
		err := p.Kill()
		if err != nil && !errors.Is(err, os.ErrProcessDone) {
			res = errors.Join(res, fmt.Errorf("failed to kill process: %w", err))
		}
	}
	if s.Stdin != nil {
		res = errors.Join(res, s.Stdin.Close())
	}

	s.Complete.Wait()
	close(s.OutputChan)
	return res
}

func (s *runningState) Output() <-chan *stateOutput {
	return s.OutputChan
}

func (s *runningState) waitDone() {
	defer func() {
		log.Printf("waitDone done")
	}()
	err := s.Cmd.Wait()
	log.Printf("waitDone err: %v", err)
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if ok {
			s.OutputChan <- &stateOutput{
				Response: &protocol.SpawnResponse{
					Payload: &protocol.SpawnResponse_Exit_{
						Exit: &protocol.SpawnResponse_Exit{Code: int32(exitErr.ExitCode())},
					},
				},
			}
		} else {
			s.OutputChan <- &stateOutput{
				Error: fmt.Errorf("failed to wait for command: %w", err),
			}
		}
	} else {
		s.OutputChan <- &stateOutput{
			Response: &protocol.SpawnResponse{
				Payload: &protocol.SpawnResponse_Exit_{
					Exit: &protocol.SpawnResponse_Exit{Code: 0},
				},
			},
		}
	}
	s.OutputChan <- &stateOutput{
		Complete: true,
	}
}

const readBufSize = 4096

func (s *runningState) readOutput(reader io.ReadCloser, newResponse func([]byte) *protocol.SpawnResponse) {
	defer func() {
		_ = reader.Close()
	}()
	var buf [readBufSize]byte
	for {
		n, err := reader.Read(buf[:])
		if n > 0 {
			s.OutputChan <- &stateOutput{
				Response: newResponse(buf[:n]),
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				s.OutputChan <- &stateOutput{
					Error: fmt.Errorf("failed to read output: %w", err),
				}
			}
			break
		}
	}
}

func (s *runningState) startIOGoRoutines(cleanPath string) {
	s.Complete.Add(1)
	go func() {
		defer s.Complete.Done()
		s.waitDone()
		log.Printf("cleanPath: %s", cleanPath)

		go func() {
			for {
				log.Printf("cleanPath from here: %s", cleanPath)
				err := os.RemoveAll(cleanPath)
				if err != nil {
					log.Printf("failed to remove dir: %s", err)
					time.Sleep(time.Minute)
					continue
				}
				break
			}

		}()
	}()

	s.Complete.Add(1)
	go func() {
		defer s.Complete.Done()
		s.readOutput(s.Stdout, func(buf []byte) *protocol.SpawnResponse {
			return &protocol.SpawnResponse{Payload: &protocol.SpawnResponse_Stdout_{
				Stdout: &protocol.SpawnResponse_Stdout{Stdout: append([]byte(nil), buf...)}}}
		})
	}()
	if s.Stderr != nil {
		s.Complete.Add(1)
		go func() {
			defer s.Complete.Done()
			s.readOutput(s.Stderr, func(bytes []byte) *protocol.SpawnResponse {
				return &protocol.SpawnResponse{Payload: &protocol.SpawnResponse_Stderr_{
					Stderr: &protocol.SpawnResponse_Stderr{Stderr: append([]byte(nil), bytes...)},
				}}
			})
		}()
	}
}

func newRunningState(ctx context.Context, head *protocol.SpawnRequest_Head, cleanPath bool) (s *runningState, err error) {
	cmd := exec.CommandContext(ctx, head.Command, head.Args...)
	cmd.Dir = head.Path
	cmd.Env = append([]string(nil), os.Environ()...)
	for _, env := range head.Envs {
		cmd.Env = append(cmd.Env, env.Key+"="+env.Value)
	}

	s = &runningState{
		Cmd: cmd,
	}
	defer func(s *runningState) {
		if err != nil {
			err = errors.Join(err, s.Close())
		}
	}(s)

	outChan := make(chan *stateOutput, 1)
	s.OutputChan = outChan
	if head.AllocatePty {
		col := head.GetWindowSize().GetCol()
		if col == 0 {
			col = 24
		}
		row := head.GetWindowSize().GetRow()
		if row == 0 {
			row = 80
		}
		log.Printf("Starting command with pty, cols: %d, rows: %d", col, row)
		var pty_ *os.File
		pty_, err = pty.StartWithSize(cmd, &pty.Winsize{Cols: uint16(col), Rows: uint16(row)})
		if err != nil {
			return nil, fmt.Errorf("failed to start command with pty: %w", err)
		}

		pr, pw := io.Pipe()
		go func() {
			defer pw.Close()
			io.Copy(pw, pty_)
		}()
		pr2, pw2 := io.Pipe()
		go func() {
			defer pr2.Close()
			io.Copy(pty_, pr2)
		}()

		s.Stdout = pr
		s.Stderr = nil
		s.Stdin = pw2
	} else {
		cmd.SysProcAttr = &syscall.SysProcAttr{
			Setpgid: true,
			Pgid:    0,
		}
		s.Stdout, err = cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		s.Stderr, err = cmd.StderrPipe()
		if err != nil {
			return nil, err
		}
		if head.HasStdin {
			s.Stdin, err = cmd.StdinPipe()
			if err != nil {
				return nil, err
			}
		}
		err = cmd.Start()
		log.Printf("Start process %d", cmd.Process.Pid)

		if err != nil {
			return nil, fmt.Errorf("failed to start command: %w", err)
		}
	}
	s.ID = uuid.New().String()
	outChan <- &stateOutput{
		Response: &protocol.SpawnResponse{
			Payload: &protocol.SpawnResponse_Pid{
				Pid: &protocol.PID{Id: s.ID},
			},
		},
	}
	cleanPathStr := ""
	if cleanPath {
		cleanPathStr = head.Path
	}

	s.startIOGoRoutines(cleanPathStr)

	return s, nil
}
