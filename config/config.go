package config

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	ContributionsStoreAddr string `json:"contributionsStoreAddr"`
	ValidationsStoreAddr   string `json:"validationsStoreAddr"`
	KMDataStoreAddr        string `json:"kbdataStoreAddr"`
	KMMetaDataStoreAddr    string `json:"kbmetadataStoreAddr"`
	DsswresStoreAddr       string `json:"dsswresStoreAddr"`
	DsswresalocStoreAddr   string `json:"dsswresalocStoreAddr"`
	PeerID                 string `json:"peerID"`
	TOSCAImportedStoreAddr string `json:"toscaImportedStoreAddr"`

	// Additional stores initialized at startup — see app/initPeer.go.
	// These were added so the chat pipeline's create-11-stores logic has
	// real docstores to bind to, rather than nil pointers.
	WhoiswhoStoreAddr            string `json:"whoiswhoStoreAddr"`
	TOSCAADTStoreAddr            string `json:"toscaAdtStoreAddr"`
	TOSCACapacitiesStoreAddr     string `json:"toscaCapacitiesStoreAddr"`
	TOSCADeploymentPlanStoreAddr string `json:"toscaDeploymentPlanStoreAddr"`
	TOSCAEventHistoryStoreAddr   string `json:"toscaEventHistoryStoreAddr"`
}

// LoadConfig loads the persistent config file into config struct
func LoadConfig() (*Config, error) {
	file, err := os.Open(*FlagRepo + "_config")
	if err == nil {
		log.Printf("File:" + *FlagRepo + "_config does not exists, and will be created as a new instance process \n")
		//fmt.Fprintf(os.Stdout, "File:"+*FlagRepo+"_config does not exists, and will be created as a new instance process \n")
	}

	//	A config example
	//	"contributionsStoreAddr": "/orbitdb/bafyreiefaj7uijgbisqis2yj563hni366po2gjs56fruq5xpu77dr37kie/contributions",
	//	"validationsStoreAddr": "/orbitdb/bafyreifm26oe6jszvklki724ereba4cpcvjzbjvxadi4v7awg3b7ckslbq/validations",
	//	"kbdataStoreAddr": "/orbitdb/bafyreiboq7kqozbxdgbszk6aj73gtsj3voarjoj462tnwdcvbzd2es5k64/kbdata",
	//	"kbmetadataStoreAddr": "/orbitdb/bafyreigbrmo7o3wb7xizb5vlbokam5jc7zsoety2a4tnoqq2q6vguflhta/kbmetadata",
	//	"peerID": "QmQ2Tt6qssMf7rCE6hX2NRnE9Uqm4zjNGwHKFLjnBU83U8"

	if err != nil {
		if os.IsNotExist(err) {
			// default config in case none was found
			config := &Config{
				ContributionsStoreAddr: "contributions",
				ValidationsStoreAddr:   "validations",
				KMDataStoreAddr:        "kbdata",
				KMMetaDataStoreAddr:    "kbmetadata",
				//
				DsswresStoreAddr:     "dsswres",
				DsswresalocStoreAddr: "dsswresaloc",
				//
				// Additional stores — short names on first boot; will be
				// replaced with real OrbitDB addresses after orbit.Open().
				WhoiswhoStoreAddr:            "whoiswho",
				TOSCAImportedStoreAddr:       "tosca_imported",
				TOSCAADTStoreAddr:            "tosca_adt",
				TOSCACapacitiesStoreAddr:     "tosca_capacities",
				TOSCADeploymentPlanStoreAddr: "tosca_deploymentplan",
				TOSCAEventHistoryStoreAddr:   "tosca_eventhistory",
				//
				PeerID: "",
			}
			//fmt.Fprintf(os.Stdout, "Stores contributions,validations,kbdata and kbmetadata are created from scratch \n")
			log.Printf("Stores contributions,validations,kbdata and kbmetadata are created from scratch \n")
			return config, nil
		}
		return nil, err
	}
	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	config := &Config{}
	err = json.Unmarshal(data, config)
	if err != nil {
		return nil, err
	}

	return config, nil
} //EoF load config

// SaveStructAsJSON writes a file in json format from a struct
func SaveStructAsJSON(s interface{}, path string) error {

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(path, data, 0644)
	if err != nil {
		return err
	}

	return nil
}
