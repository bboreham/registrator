package etcd_coatl

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"

	"github.com/bboreham/coatl/backends"
	"github.com/bboreham/coatl/data"
	"github.com/gliderlabs/registrator/bridge"
)

func init() {
	bridge.Register(new(Factory), "etcdcoatl")
}

type Factory struct{}

func (f *Factory) New(uri *url.URL) bridge.RegistryAdapter {
	urls := make([]string, 0)
	if uri.Host != "" {
		urls = append(urls, "http://"+uri.Host)
	}

	a := &CoatlAdapter{backend: backends.NewBackend(urls), services: make(map[string]*service)}
	a.readInServices()
	return a
}

type CoatlAdapter struct {
	backend  *backends.Backend
	services map[string]*service
}

type service struct {
	name    string
	details data.Service
}

func (r *CoatlAdapter) readInServices() {
	r.backend.ForeachServiceInstance(func(name, value string) {
		s := &service{name: name}
		if err := json.Unmarshal([]byte(value), &s.details); err != nil {
			log.Fatal("Error unmarshalling: ", err)
		}
		r.services[name] = s
	}, nil)
}

func (r *CoatlAdapter) Ping() error {
	return r.backend.Ping()
}

func (r *CoatlAdapter) Register(service *bridge.Service) error {
	if err := r.backend.CheckRegisteredService(r.serviceName(service)); err != nil {
		return fmt.Errorf("coatl: service not registered: %s", r.serviceName(service))
	}
	err := r.backend.AddInstance(r.serviceName(service), r.instanceName(service), service.IP, service.Port)
	if err != nil {
		log.Println("coatl: failed to register service:", err)
	}
	return err
}

func (r *CoatlAdapter) Deregister(service *bridge.Service) error {
	if r.backend.CheckRegisteredService(r.serviceName(service)) != nil {
		return nil
	}
	err := r.backend.RemoveInstance(r.serviceName(service), r.instanceName(service))
	if err != nil {
		log.Println("coatl: failed to deregister service:", err)
	}
	return err
}

func (r *CoatlAdapter) Refresh(service *bridge.Service) error {
	return r.Register(service)
}

func (r *CoatlAdapter) serviceName(service *bridge.Service) string {
	// Remove port number that Registrator helpfully adds, sometimes
	suffix := "-" + service.Origin.ExposedPort
	name := strings.TrimSuffix(service.Name, suffix)
	// If this is a service that has been registered against a specific image name, override
	for serviceName, service := range r.services {
		if name == service.details.Image {
			name = serviceName
			break
		}
	}
	return name
}

func (r *CoatlAdapter) instanceName(service *bridge.Service) string {
	return service.ID
}
