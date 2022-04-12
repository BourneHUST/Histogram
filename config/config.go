package config

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
)

type config struct {
	MaxMemory        int `yaml:"MaxMemory"`
	MinBucketSize    int `yaml:"MinBucketSize"`
	MaxBucketSize    int `yaml:"MaxBucketSize"`
	MaxLength        int `yaml:"MaxLength"`
	LengthSampleSize int `yaml:"LengthSampleSize"`
	HybridSampleSize int `yaml:"HybridSampleSize"`
	NDVSampleSize    int `yaml:"NDVSampleSize"`
	AutoSampling     int `yaml:"AutoSampling"`
}

func (c *config) GetLengthSampleSize() int {
	return c.LengthSampleSize
}
func (c *config) GetConf() {
	yamlFile, err := ioutil.ReadFile("conf.yaml")
	if err != nil {
		log.Panicln(err.Error())
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Panicln(err)
	}
}
func (c *config) TopK(size int) int {
	return c.MaxMemory * 1024 * 1024 / (size + 80)
}
func NewConfig() *config {
	ans := &config{}
	ans.GetConf()
	return ans
}

type Dsn struct {
	Addr     string
	User     string
	Password string
	Host     string
}

func (d Dsn) FormatDSN() string {
	con := fmt.Sprintf("%s:%s@tcp(%s:%s)/", d.User, d.Password, d.Addr, d.Host)
	return con
}

func NewDsn() Dsn {
	dsn := Dsn{}
	var str string
	fmt.Println("--------------开始-------------")

	fmt.Print("Please input Address: ")
	_, err := fmt.Scanln(&str)
	if err != nil {
		log.Panicln(err.Error())
	}
	dsn.Addr = str

	fmt.Print("Please input User Name: ")
	_, err = fmt.Scanln(&str)
	if err != nil {
		log.Panicln(err.Error())
	}
	dsn.User = str

	fmt.Print("Please input Password: ")
	_, err = fmt.Scanln(&str)
	if err != nil {
		log.Panicln(err.Error())
	}
	dsn.Password = str

	return dsn
}
