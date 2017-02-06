package transport

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	"gitlab.fg/otis/iris"
	"gitlab.fg/otis/iris/mapsource"
	"gitlab.fg/otis/iris/pb"
	"golang.org/x/net/context"
)

// SourceFactory describes a method that returns a new source with the provided identifier
type SourceFactory func(identifier string) iris.Source

// SessionMap is a map used to efficiently store and search for sessions
type SessionMap map[string]struct{}

// Session represents an server-side update stream
type Session struct {
	ID       string
	Listener pb.Iris_ListenServer
}

// Server implements the generated pb.IrisServer interface
type Server struct {
	initialized     bool                             //indicates whether Init has been called
	SourceFactory   SourceFactory                    //factory method for creating sources
	sources         map[string]iris.Source           //collection of sources accessed by identifier
	sourcesMutex    *sync.Mutex                      //used when managing our collection of sources
	sessions        map[string]*Session              //collection of sessions
	sessionsMutex   *sync.Mutex                      //used to lock the sessions collection
	sourceSubs      map[string]SessionMap            //collection of sessions subscribed to sources
	sourceSubsMutex *sync.Mutex                      //used to lock the source subscriptions collection
	keySubs         map[string]map[string]SessionMap //collection of sessions subscribed to a source and key
	keySubsMutex    *sync.Mutex                      //used to lock the key subscriptions collection
}

//initialize the server's caching/state mechanisms
func (s *Server) initialize() {
	if s.initialized {
		return
	}

	s.initialized = true
	s.sourcesMutex = &sync.Mutex{}
	s.sessionsMutex = &sync.Mutex{}
	s.sourceSubsMutex = &sync.Mutex{}
	s.keySubsMutex = &sync.Mutex{}
}

// Connect responds with a stream of objects representing source, key, value updates
func (s *Server) Connect(ctx context.Context, req *pb.ConnectRequest) (*pb.ConnectResponse, error) {
	s.initialize()

	session, err := s.generateSessionID(10)
	if err != nil {
		return nil, fmt.Errorf("Unable to generate session identifier. %s", err)
	}

	if _, err := s.addSession(session, nil); err != nil {
		return nil, err
	}

	return &pb.ConnectResponse{
		Session: session,
	}, nil
}

// Listen responds with a stream of objects representing source, key, value updates
func (s *Server) Listen(req *pb.ListenRequest, stream pb.Iris_ListenServer) error {
	s.initialize()

	if _, err := s.addSession(req.Session, stream); err != nil {
		return err
	}

	<-stream.Context().Done()
	s.removeSession(req.Session)
	return stream.Context().Err()
}

// GetSources responds with a stream of objects representing available sources
func (s *Server) GetSources(req *pb.GetSourcesRequest, stream pb.Iris_GetSourcesServer) error {
	s.initialize()

	s.sourcesMutex.Lock()
	defer s.sourcesMutex.Unlock()

	for source := range s.sources {
		if err := stream.Send(&pb.GetSourcesResponse{Source: source}); err != nil {
			return err
		}
	}
	return nil
}

// GetKeys responds with a stream of objects representing available sources
func (s *Server) GetKeys(req *pb.GetKeysRequest, stream pb.Iris_GetKeysServer) error {
	s.initialize()

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

// SetValue sets the value for the specified source and key
func (s *Server) SetValue(c context.Context, req *pb.SetValueRequest) (*pb.SetValueResponse, error) {
	s.initialize()

	source := s.getSourceWithIdentifier(req.Source)
	if source == nil {
		return nil, errors.New("Source could not be found")
	}

	err := source.Set(req.Key, req.Value)
	if err != nil {
		return nil, err
	}

	go s.publish(req.Source, req.Key, req.Value)

	return &pb.SetValueResponse{
		Value: req.Value,
	}, nil
}

// GetValue expects a source and key and responds with the associated value
func (s *Server) GetValue(c context.Context, req *pb.GetValueRequest) (*pb.GetValueResponse, error) {
	s.initialize()

	source := s.getSourceWithIdentifier(req.Source)
	if source == nil {
		return nil, errors.New("Source could not be found")
	}

	value, err := source.Get(req.Key)
	if err != nil {
		return nil, err
	}

	return &pb.GetValueResponse{
		Value: value,
	}, nil
}

// RemoveValue removes the specified value from the provided source
func (s *Server) RemoveValue(ctx context.Context, req *pb.RemoveValueRequest) (*pb.RemoveValueResponse, error) {
	s.initialize()

	if len(req.Source) == 0 {
		return nil, errors.New("You must provide the identifier of source you would like to be removed")
	}

	if len(req.Key) == 0 {
		return nil, errors.New("You must provide the key of the value you would like to be removed")
	}

	s.sourcesMutex.Lock()
	defer s.sourcesMutex.Unlock()

	if s.sources[req.Source] != nil {
		if err := s.sources[req.Source].Remove(req.Key); err != nil {
			return nil, err
		}
	}

	return &pb.RemoveValueResponse{
		Session: req.Session,
		Source:  req.Source,
		Key:     req.Key,
	}, nil
}

// RemoveSource removes the specified source and all of its contents
func (s *Server) RemoveSource(ctx context.Context, req *pb.RemoveSourceRequest) (*pb.RemoveSourceResponse, error) {
	s.initialize()

	if len(req.Source) == 0 {
		return nil, errors.New("You must provide the identifier of source you would like to be removed")
	}

	s.sourcesMutex.Lock()
	defer s.sourcesMutex.Unlock()

	delete(s.sources, req.Source)
	return &pb.RemoveSourceResponse{
		Session: req.Session,
		Source:  req.Source,
	}, nil
}

// Subscribe indicates that the client wishes to be notified of all updates for the specified source
func (s *Server) Subscribe(ctx context.Context, req *pb.SubscribeRequest) (*pb.SubscribeResponse, error) {
	s.initialize()

	if len(req.Session) == 0 {
		return nil, errors.New("Subscribe requires that you provide a valid session")
	}

	if len(req.Source) == 0 {
		return nil, errors.New("Subscribe requires that you provide a source")
	}

	s.sourceSubsMutex.Lock()
	defer s.sourceSubsMutex.Unlock()

	if s.sourceSubs == nil {
		s.sourceSubs = make(map[string]SessionMap)
	}

	if s.sourceSubs[req.Source] == nil {
		s.sourceSubs[req.Source] = make(SessionMap)
	}

	var empty struct{}
	s.sourceSubs[req.Source][req.Session] = empty
	return &pb.SubscribeResponse{Source: req.Source}, nil
}

// SubscribeKey indicates that the client wishes to be notified of updates associated with
// a specific key from the specified source
func (s *Server) SubscribeKey(ctx context.Context, req *pb.SubscribeKeyRequest) (*pb.SubscribeKeyResponse, error) {
	s.initialize()

	if len(req.Session) == 0 {
		return nil, errors.New("SubscribeKey requires that you provide a valid session")
	}

	if len(req.Source) == 0 {
		return nil, errors.New("SubscribeKey requires that you provide a source")
	}

	if len(req.Key) == 0 {
		return nil, errors.New("SubscribeKey requires that you provide a key")
	}

	s.keySubsMutex.Lock()
	defer s.keySubsMutex.Unlock()

	if s.keySubs == nil {
		s.keySubs = make(map[string]map[string]SessionMap)
	}

	if s.keySubs[req.Source] == nil {
		s.keySubs[req.Source] = make(map[string]SessionMap)
	}

	if s.keySubs[req.Source][req.Key] == nil {
		s.keySubs[req.Source][req.Key] = make(SessionMap)
	}

	var empty struct{}
	s.keySubs[req.Source][req.Key][req.Session] = empty
	return &pb.SubscribeKeyResponse{Source: req.Source, Key: req.Key}, nil
}

// Unsubscribe indicates that the client no longer wishes to be notified of updates for the specified source
func (s *Server) Unsubscribe(ctx context.Context, req *pb.UnsubscribeRequest) (*pb.UnsubscribeResponse, error) {
	s.initialize()

	if len(req.Session) == 0 {
		return nil, errors.New("Unsubscribe requires that you provide a session")
	}

	if len(req.Source) == 0 {
		return nil, errors.New("Unsubscribe requires that you provide a source")
	}

	s.sourceSubsMutex.Lock()
	defer s.sourceSubsMutex.Unlock()

	if s.sourceSubs == nil || s.sourceSubs[req.Source] == nil {
		return &pb.UnsubscribeResponse{}, nil
	}

	if _, ok := s.sourceSubs[req.Source][req.Session]; !ok {
		return &pb.UnsubscribeResponse{}, nil
	}

	delete(s.sourceSubs[req.Source], req.Session)
	return &pb.UnsubscribeResponse{Source: req.Source}, nil
}

// UnsubscribeKey indicates that the client no longer wishes to be notified of updates associated
// with a specific key from the specified source
func (s *Server) UnsubscribeKey(ctx context.Context, req *pb.UnsubscribeKeyRequest) (*pb.UnsubscribeKeyResponse, error) {
	s.initialize()

	if len(req.Session) == 0 {
		return nil, errors.New("UnsubscribeKey requires that you provide a valid session")
	}

	if len(req.Source) == 0 {
		return nil, errors.New("UnsubscribeKey requires that you provide a source")
	}

	if len(req.Key) == 0 {
		return nil, errors.New("UnsubscribeKey requires that you provide a key")
	}

	s.keySubsMutex.Lock()
	defer s.keySubsMutex.Unlock()

	if s.keySubs == nil || s.keySubs[req.Source] == nil || s.keySubs[req.Source][req.Key] == nil {
		return &pb.UnsubscribeKeyResponse{}, nil
	}

	if _, ok := s.keySubs[req.Source][req.Key][req.Session]; !ok {
		return &pb.UnsubscribeKeyResponse{}, nil
	}

	delete(s.keySubs[req.Source][req.Key], req.Session)
	return &pb.UnsubscribeKeyResponse{Source: req.Source, Key: req.Key}, nil
}

// Sends the provided value to any streams subscribed to the specified source and key
func (s *Server) publish(source string, key string, value []byte) error {
	s.initialize()

	var update = &pb.Update{
		Source: source,
		Key:    key,
		Value:  value,
	}

	var notify = func(identifier string, update *pb.Update) error {
		stream, ok := s.sessions[identifier]
		if ok {
			if stream.Listener != nil {
				if err := stream.Listener.Send(update); err != nil {
					return err
				}
			}
		}

		return nil
	}

	var returnErrors []error

	s.sourceSubsMutex.Lock()
	if s.sourceSubs != nil && s.sourceSubs[source] != nil {
		for identifier := range s.sourceSubs[source] {
			if err := notify(identifier, update); err != nil {
				returnErrors = append(returnErrors, err)
			}
		}
	}
	s.sourceSubsMutex.Unlock()

	s.keySubsMutex.Lock()
	if s.keySubs != nil && s.keySubs[source] != nil && s.keySubs[source][key] != nil {
		for identifier := range s.keySubs[source][key] {
			if err := notify(identifier, update); err != nil {
				returnErrors = append(returnErrors, err)
			}
		}
	}
	s.keySubsMutex.Unlock()

	if len(returnErrors) > 0 {
		return errors.New("An issue was encountered attempting to send updates to some clients")
	}

	return nil
}

// generateSessionID produces a unique session identifier for this server
func (s *Server) generateSessionID(length int) (string, error) {
	s.initialize()

	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}

	session := fmt.Sprintf("%X", b)

	s.sessionsMutex.Lock()
	defer s.sessionsMutex.Unlock()

	if s.sessions == nil {
		s.sessions = make(map[string]*Session)
	}

	if _, ok := s.sessions[session]; ok {
		return s.generateSessionID(length)
	}

	return session, nil
}

// addSession adds the session to the server's collection
func (s *Server) addSession(sessionIdentifier string, listener pb.Iris_ListenServer) (*Session, error) {
	s.initialize()

	s.sessionsMutex.Lock()
	defer s.sessionsMutex.Unlock()

	if s.sessions == nil {
		s.sessions = make(map[string]*Session)
	}

	session := &Session{ID: sessionIdentifier, Listener: listener}
	s.sessions[sessionIdentifier] = session

	return session, nil
}

// removeSession removes the session from the server's collection
func (s *Server) removeSession(sessionIdentifier string) error {
	s.initialize()

	s.sourceSubsMutex.Lock()
	if s.sourceSubs != nil {
		delete(s.sourceSubs, sessionIdentifier)
	}
	s.sourceSubsMutex.Unlock()

	s.keySubsMutex.Lock()
	if s.keySubs != nil {
		delete(s.keySubs, sessionIdentifier)
	}
	s.keySubsMutex.Unlock()

	s.sessionsMutex.Lock()
	if s.sessions != nil {
		delete(s.sessions, sessionIdentifier)
	}

	s.sessionsMutex.Unlock()

	return nil
}

// getSourceWithIdentifier returns the source with the provided identifier, or the existing one if already created
func (s *Server) getSourceWithIdentifier(identifier string) iris.Source {
	s.initialize()

	s.sourcesMutex.Lock()
	defer s.sourcesMutex.Unlock()

	if s.sources == nil {
		s.sources = make(map[string]iris.Source)
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
