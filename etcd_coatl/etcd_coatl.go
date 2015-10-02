package etcd_coatl

import (
	"encoding/json"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/bboreham/coatl/backend"
	"github.com/bboreham/coatl/data"
	"github.com/coreos/go-etcd/etcd"
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

	a := &CoatlAdapter{client: etcd.NewClient(urls), services: make(map[string]*service)}
	a.readInServices()
	return a
}

type CoatlAdapter struct {
	client   *etcd.Client
	services map[string]*service
}

type service struct {
	name    string
	details data.Service
}

func (r *CoatlAdapter) readInServices() {
	services := make(map[string]*service)
	backend.ForeachServiceInstance(func(name, value string) {
		s := &service{name: name}
		if err := json.Unmarshal([]byte(value), &s.details); err != nil {
			log.Fatal("Error unmarshalling: ", err)
		}
		services[name] = s
	}, nil)
}

func (r *CoatlAdapter) Ping() error {
	rr := etcd.NewRawRequest("GET", "version", nil, nil)
	_, err := r.client.SendRequest(rr)
	if err != nil {
		return err
	}
	return nil
}

func (r *CoatlAdapter) Register(service *bridge.Service) error {
	if !r.isRegisteredService(service) {
		return nil
	}
	port := strconv.Itoa(service.Port)
	record := `{"address":"` + service.IP + `","port":` + port + `}`
	_, err := r.client.Set(r.instancePath(service), record, uint64(service.TTL))
	if err != nil {
		log.Println("coatl: failed to register service:", err)
	}
	return err
}

func (r *CoatlAdapter) Deregister(service *bridge.Service) error {
	if !r.isRegisteredService(service) {
		return nil
	}
	_, err := r.client.Delete(r.instancePath(service), false)
	if err != nil {
		log.Println("coatl: failed to deregister service:", err)
	}
	return err
}

func (r *CoatlAdapter) Refresh(service *bridge.Service) error {
	return r.Register(service)
}

func (r *CoatlAdapter) servicePath(service *bridge.Service) string {
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
	return data.ServicePath + name + "/"
}

func (r *CoatlAdapter) instancePath(service *bridge.Service) string {
	return r.servicePath(service) + service.ID
}

func (r *CoatlAdapter) isRegisteredService(service *bridge.Service) bool {
	_, err := r.client.Get(r.servicePath(service)+"_details", false, false)
	return err == nil
}
