package server

import (
	"errors"
	"sync"

	"gitlab.fg/otis/sourcehub"
	"gitlab.fg/otis/sourcehub/mapsource"
	"golang.org/x/net/context"
)

//SourceFactory describes a method that returns a new source with the provided identifier
type SourceFactory func(identifier string) sourcehub.Source

//Server implements the generated sourcehub.SourceHubServer interface
type Server struct {
	SourceFactory SourceFactory               //factory method for creating sources
	sources       map[string]sourcehub.Source //collection of sources accessed by identifier
	sourcesMutex  *sync.Mutex                 //used when managing our collection of sources
}

// getSourceWithIdentifier returns the source with the provided identifier, or the existing one if already created
func (s *Server) getSourceWithIdentifier(identifier string) sourcehub.Source {
	if s.sourcesMutex == nil {
		s.sourcesMutex = &sync.Mutex{}
	}

	s.sourcesMutex.Lock()
	defer s.sourcesMutex.Unlock()

	if s.sources == nil {
		s.sources = make(map[string]sourcehub.Source)
	}

	source := s.sources[identifier]
	if source != nil {
		return source
	}

	if s.SourceFactory == nil {
		source = mapsource.NewMapSource(identifier)
	} else {
		source = s.SourceFactory(identifier)
	}
	s.sources[identifier] = source
	return source
}

// GetValue expects a source and key and responds with the associated value
func (s *Server) GetValue(c context.Context, req *sourcehub.GetValueRequest) (*sourcehub.GetValueResponse, error) {
	source := s.getSourceWithIdentifier(req.Source)
	if source == nil {
		return nil, errors.New("source could not be found")
	}

	value, err := source.Get(req.Key)
	if err != nil {
		return nil, err
	}

	return &sourcehub.GetValueResponse{
		Value: value,
	}, nil
}

// SetValue sets the value for the specified source and key
func (s *Server) SetValue(c context.Context, req *sourcehub.SetValueRequest) (*sourcehub.SetValueResponse, error) {
	source := s.getSourceWithIdentifier(req.Source)
	if source == nil {
		return nil, errors.New("source could not be found")
	}

	err := source.Set(req.Key, req.Value)
	if err != nil {
		return nil, err
	}

	return &sourcehub.SetValueResponse{
		Value: req.Value,
	}, nil
}

// Subscribe streams updates to a value for a given source and key
func (s *Server) Subscribe(req *sourcehub.SubscribeRequest, stream sourcehub.SourceHub_SubscribeServer) error {
	return errors.New("not implemented")
}
