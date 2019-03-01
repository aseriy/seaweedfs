package weed_server

import (
	"google.golang.org/grpc"
	"net/http"
	"os"

	"../filer2"
	_ "../filer2/cassandra"
	_ "../filer2/leveldb"
	_ "../filer2/memdb"
	_ "../filer2/mysql"
	_ "../filer2/postgres"
	_ "../filer2/redis"
	"../glog"
	"../notification"
	_ "../notification/aws_sqs"
	_ "../notification/google_pub_sub"
	_ "../notification/kafka"
	_ "../notification/log"
	"../security"
	"github.com/spf13/viper"
)

type FilerOption struct {
	Masters            []string
	Collection         string
	DefaultReplication string
	RedirectOnRead     bool
	DisableDirListing  bool
	MaxMB              int
	DirListingLimit    int
	DataCenter         string
	DefaultLevelDbDir  string
}

type FilerServer struct {
	option         *FilerOption
	secret         security.SigningKey
	filer          *filer2.Filer
	grpcDialOption grpc.DialOption
}

func NewFilerServer(defaultMux, readonlyMux *http.ServeMux, option *FilerOption) (fs *FilerServer, err error) {

	fs = &FilerServer{
		option:         option,
		grpcDialOption: security.LoadClientTLS(viper.Sub("grpc"), "filer"),
	}

	if len(option.Masters) == 0 {
		glog.Fatal("master list is required!")
	}

	fs.filer = filer2.NewFiler(option.Masters, fs.grpcDialOption)

	go fs.filer.KeepConnectedToMaster()

	v := viper.GetViper()
	if !LoadConfiguration("filer", false) {
		v.Set("leveldb.enabled", true)
		v.Set("leveldb.dir", option.DefaultLevelDbDir)
		_, err := os.Stat(option.DefaultLevelDbDir)
		if os.IsNotExist(err) {
			os.MkdirAll(option.DefaultLevelDbDir, 0755)
		}
	}
	LoadConfiguration("notification", false)

	fs.filer.LoadConfiguration(v)

	notification.LoadConfiguration(v.Sub("notification"))

	handleStaticResources(defaultMux)
	defaultMux.HandleFunc("/", fs.filerHandler)
	if defaultMux != readonlyMux {
		readonlyMux.HandleFunc("/", fs.readonlyFilerHandler)
	}

	return fs, nil
}

func (fs *FilerServer) jwt(fileId string) security.EncodedJwt {
	return security.GenJwt(fs.secret, fileId)
}

func LoadConfiguration(configFileName string, required bool) (loaded bool) {

	// find a filer store
	viper.SetConfigName(configFileName)     // name of config file (without extension)
	viper.AddConfigPath(".")                // optionally look for config in the working directory
	viper.AddConfigPath("$HOME/.seaweedfs") // call multiple times to add many search paths
	viper.AddConfigPath("/etc/seaweedfs/")  // path to look for the config file in

	glog.V(0).Infof("Reading %s.toml from %s", configFileName, viper.ConfigFileUsed())

	if err := viper.MergeInConfig(); err != nil { // Handle errors reading the config file
		glog.V(0).Infof("Reading %s: %v", viper.ConfigFileUsed(), err)
		if required {
			glog.Fatalf("Failed to load %s.toml file from current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/"+
				"\n\nPlease follow this example and add a filer.toml file to "+
				"current directory, or $HOME/.seaweedfs/, or /etc/seaweedfs/:\n"+
				"    https://github.com/chrislusf/seaweedfs/blob/master/weed/%s.toml\n"+
				"\nOr use this command to generate the default toml file\n"+
				"    weed scaffold -config=%s -output=.\n\n\n",
				configFileName, configFileName, configFileName)
		} else {
			return false
		}
	}

	return true

}
