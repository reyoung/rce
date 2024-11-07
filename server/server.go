package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/reyoung/rce/process"
	"github.com/reyoung/rce/protocol"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

var (
	preSpawnCmd = os.Getenv("RCE_PRE_SPAWN_CMD")
)

type Server struct {
	protocol.UnimplementedRemoteCodeExecutorServer

	processes map[string]process.Process
	mutex     sync.RWMutex
}

func forceClose(c chan struct{}) {
	defer func() {
		recover()
	}()
	close(c)
}

func runPreSpawnCmd(ctx context.Context) ([]byte, error) {
	if preSpawnCmd == "" {
		return nil, nil
	}
	parts := strings.Fields(preSpawnCmd)
	head := parts[0]
	parts = parts[1:]

	return exec.CommandContext(ctx, head, parts...).Output()
}

type processSetter struct {
	pid string
	s   *Server
	p   process.Process
}

func (p *processSetter) TrySet() {
	if p.pid != "" { // already set
		return
	}
	p.pid = p.p.PID()
	if p.pid == "" { // not started
		return
	}
	p.s.mutex.Lock()
	defer p.s.mutex.Unlock()
	if p.s.processes == nil {
		p.s.processes = make(map[string]process.Process)
	}
	p.s.processes[p.pid] = p.p
}

func (p *processSetter) Unset() {
	if p.pid == "" {
		return
	}
	p.s.mutex.Lock()
	defer p.s.mutex.Unlock()
	delete(p.s.processes, p.pid)
}

func (s *Server) Spawn(svr protocol.RemoteCodeExecutor_SpawnServer) error {
	out, e := runPreSpawnCmd(svr.Context())
	if e != nil {
		return errors.Join(e, fmt.Errorf("fail to run pre-spwan cmd '%s'", preSpawnCmd))
	}
	if out != nil {
		log.Printf("Complete running pre-spawn command. output: %s", out)
	}
	p := process.New(svr.Context())
	defer func() {
		log.Printf("Closing process")
		_ = p.Close()
	}()

	exited := make(chan struct{})
	var complete sync.WaitGroup
	complete.Add(2) // 2 means send/recv

	go func() {
		defer func() {
			complete.Done()
			log.Printf("Writing request goroutine exit")
		}()
		for {
			req, err := svr.Recv()
			if err != nil {
				forceClose(exited)
				return
			}
			log.Printf("Received request: %T", req.GetPayload())
			p.RequestChan() <- req
		}
	}()

	var err error
	pidSetter := &processSetter{s: s, p: p}
	defer pidSetter.Unset()

	go func() {
		defer func() {
			log.Printf("Reading response goroutine exit")
			forceClose(exited)
			complete.Done()

			go func() {
				// read until close
				for range p.ResponseChan() {
				}
			}()

		}()
		for {
			var ok bool
			select {
			case rsp := <-p.ResponseChan():
				pidSetter.TrySet()
				err = svr.Send(rsp)
				if err != nil {
					return
				}

			case err, ok = <-p.ErrorChan():
				if !ok {
					return
				}

				rsp := &protocol.SpawnResponse{Payload: &protocol.SpawnResponse_Error{
					Error: &protocol.SpawnResponse_SystemError{Error: err.Error()}}}
				err = errors.Join(err, svr.Send(rsp))
				return
			case <-exited:
				return
			}
		}
	}()
	complete.Wait()
	return err
}

func (s *Server) Kill(ctx context.Context, pid *protocol.PID) (*protocol.KillResponse, error) {
	log.Printf("Received kill request: %v", pid.String())
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	p, ok := s.processes[pid.Id]
	if !ok {
		return &protocol.KillResponse{Error: "process not found"}, nil
	}
	err := p.Kill()
	if err != nil {
		return &protocol.KillResponse{Error: err.Error()}, nil
	}
	return nil, nil
}
