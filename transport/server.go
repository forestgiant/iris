package transport

import (
	"errors"
	"sync"

	"gitlab.fg/otis/sourcehub"
	"gitlab.fg/otis/sourcehub/mapsource"
	"gitlab.fg/otis/sourcehub/pb"
	"golang.org/x/net/context"
)

//SourceFactory describes a method that returns a new source with the provided identifier
type SourceFactory func(identifier string) sourcehub.Source

//Server implements the generated sourcehub.SourceHubServer interface
type Server struct {
	SourceFactory SourceFactory               //factory method for creating sources
	sources       map[string]sourcehub.Source //collection of sources accessed by identifier
	sourcesMutex  *sync.Mutex                 //used when managing our collection of sources

	subscriptions      map[string]map[string]map[*pb.SubscribeRequest]pb.SourceHub_SubscribeServer
	subscriptionsMutex *sync.Mutex
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

// GetSources responds with a stream of objects representing available sources
func (s *Server) GetSources(req *pb.GetSourcesRequest, stream pb.SourceHub_GetSourcesServer) error {
	s.sourcesMutex.Lock()
	defer s.sourcesMutex.Unlock()

	for source := range s.sources {
		if err := stream.Send(&pb.GetSourcesResponse{Source: source}); err != nil {
			return err
		}
	}
	return nil
}

// GetValue expects a source and key and responds with the associated value
func (s *Server) GetValue(c context.Context, req *pb.GetValueRequest) (*pb.GetValueResponse, error) {
	source := s.getSourceWithIdentifier(req.Source)
	if source == nil {
		return nil, errors.New("source could not be found")
	}

	value, err := source.Get(req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.GetValueResponse{
		Value: value,
	}, nil
}

// SetValue sets the value for the specified source and key
func (s *Server) SetValue(c context.Context, req *pb.SetValueRequest) (*pb.SetValueResponse, error) {
	source := s.getSourceWithIdentifier(req.Source)
	if source == nil {
		return nil, errors.New("source could not be found")
	}

	err := source.Set(req.Key, req.Value)
	if err != nil {
		return nil, err
	}

	if err := s.notifySubscriptionsOfValue(req.Source, req.Key, req.Value); err != nil {
		return nil, err
	}

	return &pb.SetValueResponse{
		Value: req.Value,
	}, nil
}

// GetKeys responds with a stream of objects representing available sources
func (s *Server) GetKeys(req *pb.GetKeysRequest, stream pb.SourceHub_GetKeysServer) error {
	source := s.getSourceWithIdentifier(req.Source)
	keys, err := source.GetKeys()
	if err != nil {
		return nil
	}

	for _, k := range keys {
		if err := stream.Send(&pb.GetKeysResponse{Key: k}); err != nil {
			return err
		}
	}
	return nil
}

// Sends the provided value to any streams subscribed to the specified source and key
func (s *Server) notifySubscriptionsOfValue(source string, key string, value []byte) error {
	for _, stream := range s.subscriptions[source][key] {
		err := stream.Send(&pb.SubscribeResponse{
			Value: value,
		})

		if err != nil {
			return err
		}
	}

	return nil
}

// Subscribe streams updates to a value for a given source and key
func (s *Server) Subscribe(req *pb.SubscribeRequest, stream pb.SourceHub_SubscribeServer) error {
	//Validate the request
	if len(req.Source) == 0 {
		return errors.New("source is a required parameter")
	}

	if len(req.Key) == 0 {
		return errors.New("key is a required parameter")
	}

	//Lock the mutex protecting our storage
	if s.subscriptionsMutex == nil {
		s.subscriptionsMutex = &sync.Mutex{}
	}
	s.subscriptionsMutex.Lock()

	//Listen for updates to the specified source until the stream is closed
	if s.subscriptions == nil {
		s.subscriptions = make(map[string]map[string]map[*pb.SubscribeRequest]pb.SourceHub_SubscribeServer)
	}
	if s.subscriptions[req.Source] == nil {
		s.subscriptions[req.Source] = make(map[string]map[*pb.SubscribeRequest]pb.SourceHub_SubscribeServer)
	}
	if s.subscriptions[req.Source][req.Key] == nil {
		s.subscriptions[req.Source][req.Key] = make(map[*pb.SubscribeRequest]pb.SourceHub_SubscribeServer)
	}
	s.subscriptions[req.Source][req.Key][req] = stream
	s.subscriptionsMutex.Unlock()
	<-stream.Context().Done()

	//Remove our subscription when we are finished with it
	s.subscriptionsMutex.Lock()
	delete(s.subscriptions[req.Source][req.Key], req)
	s.subscriptionsMutex.Unlock()
	return nil
}
