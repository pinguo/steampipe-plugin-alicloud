package alicloud

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth"
	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/auth/credentials"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/actiontrail"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/alb"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/alidns"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/alikafka"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/bssopenapi"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cas"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cms"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cs"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ecs"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ess"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/kms"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/r_kvstore"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/ram"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/rds"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/sas"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/slb"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/sts"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/vpc"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	ossCred "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	sls "github.com/aliyun/aliyun-log-go-sdk"

	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
)

// ALBService returns the service connection for Alicloud Application Load Balancer service
func ALBService(ctx context.Context, d *plugin.QueryData) (*alb.Client, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	if region == "" {
		return nil, fmt.Errorf("region must be passed ALBService")
	}

	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("alb-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*alb.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := alb.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// AliDNSService returns the service connection for Alicloud DNS service
func AliDNSService(ctx context.Context, d *plugin.QueryData) (*alidns.Client, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	if region == "" {
		return nil, fmt.Errorf("region must be passed AliDNSService")
	}

	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("alidns-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*alidns.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := alidns.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// AlikafkaService returns the service connection for Alicloud Kafka service
func AlikafkaService(ctx context.Context, d *plugin.QueryData) (*alikafka.Client, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	if region == "" {
		return nil, fmt.Errorf("region must be passed AlikafkaService")
	}

	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("alikafka-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*alikafka.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := alikafka.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// AutoscalingService returns the service connection for Alicloud Autoscaling service
func AutoscalingService(ctx context.Context, d *plugin.QueryData) (*ess.Client, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	if region == "" {
		return nil, fmt.Errorf("region must be passed AutoscalingService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("ess-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*ess.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := ess.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// BssOpenApiService returns the service connection for Alicloud BSS OpenAPI service
func BssOpenApiService(ctx context.Context, d *plugin.QueryData) (*bssopenapi.Client, error) {
	region := GetDefaultRegion(d.Connection)

	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("bssopenapi-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*bssopenapi.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := bssopenapi.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// CasService returns the service connection for Alicloud SSL service
func CasService(ctx context.Context, d *plugin.QueryData, region string) (*cas.Client, error) {
	if region == "" {
		return nil, fmt.Errorf("region must be passed CasService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("cas-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*cas.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := cas.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// CmsService returns the service connection for Alicloud CMS service
func CmsService(ctx context.Context, d *plugin.QueryData) (*cms.Client, error) {
	region := GetDefaultRegion(d.Connection)

	if region == "" {
		return nil, fmt.Errorf("region must be passed CmsService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("cms-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*cms.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := cms.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// ECSService returns the service connection for Alicloud ECS service
func ECSService(ctx context.Context, d *plugin.QueryData) (*ecs.Client, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	if region == "" {
		return nil, fmt.Errorf("region must be passed ECSService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("ecs-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*ecs.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := ecs.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// ECSRegionService returns the service connection for Alicloud ECS Region service
func ECSRegionService(ctx context.Context, d *plugin.QueryData, region string) (*ecs.Client, error) {
	if region == "" {
		return nil, fmt.Errorf("region must be passed ECSRegionService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("ecsregion-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*ecs.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := ecs.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// KMSService returns the service connection for Alicloud KMS service
func KMSService(ctx context.Context, d *plugin.QueryData) (*kms.Client, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	if region == "" {
		return nil, fmt.Errorf("region must be passed KMSService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("kms-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*kms.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := kms.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// RAMService returns the service connection for Alicloud RAM service
func RAMService(ctx context.Context, d *plugin.QueryData) (*ram.Client, error) {
	region := GetDefaultRegion(d.Connection)

	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("ram-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*ram.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := ram.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// SLBService returns the service connection for Alicloud Server Load Balancer service
func SLBService(ctx context.Context, d *plugin.QueryData) (*slb.Client, error) {
	region := GetDefaultRegion(d.Connection)

	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("slb-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*slb.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := slb.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// StsService returns the service connection for Alicloud STS service
func StsService(ctx context.Context, d *plugin.QueryData) (*sts.Client, error) {
	region := GetDefaultRegion(d.Connection)
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("sts-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*sts.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := sts.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// VpcService returns the service connection for Alicloud VPC service
func VpcService(ctx context.Context, d *plugin.QueryData) (*vpc.Client, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	if region == "" {
		return nil, fmt.Errorf("region must be passed VpcService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("vpc-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*vpc.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := vpc.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// OssService returns the service connection for Alicloud OSS service
func OssService(ctx context.Context, d *plugin.QueryData, region string) (*oss.Client, error) {
	// Validate the region parameter before proceeding
	if region == "" {
		return nil, fmt.Errorf("region must be provided to initialize the OSS service")
	}

	// Check if the OSS client is already cached to avoid redundant initialization
	serviceCacheKey := fmt.Sprintf("oss-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*oss.Client), nil
	}

	// Construct the OSS endpoint for the given region
	endpoint := "oss-" + region + ".aliyuncs.com"

	// Initialize OSS client configuration
	timeout := getClientTimeout(d)
	ossCfg := oss.NewConfig()
	ossCfg.WithEndpoint(endpoint)
	ossCfg.WithRegion(region)
	ossCfg.WithProxyFromEnvironment(true)
	ossCfg.WithConnectTimeout(timeout)
	ossCfg.WithReadWriteTimeout(timeout)

	// Retrieve cached credentials for authentication
	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve cached credentials: %v", err)
	}

	cfg := credCfg.(*CredentialConfig)

	// Convert the credential configuration to an OSS-compatible provider
	credentialProvider, err := auth.ToCredentialsProvider(cfg.Creds)
	if err != nil {
		return nil, fmt.Errorf("failed to convert credentials to a provider: %v", err)
	}

	// Retrieve credentials from the provider
	profileCred, err := credentialProvider.GetCredentials()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve credentials from the provider: %v", err)
	}

	ossCfg.CredentialsProvider = ossCred.NewStaticCredentialsProvider(profileCred.AccessKeyId, profileCred.AccessKeySecret, profileCred.SecurityToken)

	// Initialize and return the OSS client
	svc := oss.NewClient(ossCfg)

	// Cache the service connection to optimize future requests
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// ActionTrailService returns the service connection for Alicloud ActionTrail service
func ActionTrailService(ctx context.Context, d *plugin.QueryData) (*actiontrail.Client, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	if region == "" {
		return nil, fmt.Errorf("region must be passed ActionTrailService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("actiontrail-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*actiontrail.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := actiontrail.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// ContainerService returns the service connection for Alicloud Container service
func ContainerService(ctx context.Context, d *plugin.QueryData) (*cs.Client, error) {
	region := GetDefaultRegion(d.Connection)
	return ContainerServiceWithRegion(ctx, d, region)
}

// ContainerServiceWithRegion returns the service connection for Alicloud Container service with a specific region
func ContainerServiceWithRegion(ctx context.Context, d *plugin.QueryData, region string) (*cs.Client, error) {
	if region == "" {
		return nil, fmt.Errorf("region must be passed ContainerService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("cs-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*cs.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := cs.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// SecurityCenterService returns the service connection for Alicloud Security Center service
func SecurityCenterService(ctx context.Context, d *plugin.QueryData, region string) (*sas.Client, error) {
	if region == "" {
		return nil, fmt.Errorf("region must be passed SecurityCenterService")
	}

	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("sas-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*sas.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := sas.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// RDSService returns the service connection for Alicloud RDS service
func RDSService(ctx context.Context, d *plugin.QueryData, region string) (*rds.Client, error) {
	if region == "" {
		return nil, fmt.Errorf("region must be passed RDSService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("rds-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*rds.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := rds.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	// Set default read/connect timeout to 60s if not configured
	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// RedisService returns the service connection for Alicloud Redis (R-KVStore) service
func RedisService(ctx context.Context, d *plugin.QueryData, region string) (*r_kvstore.Client, error) {
	if region == "" {
		return nil, fmt.Errorf("region must be passed RedisService")
	}
	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("redis-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(*r_kvstore.Client), nil
	}

	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, err
	}
	cfg := credCfg.(*CredentialConfig)

	// so it was not in cache - create service
	svc, err := r_kvstore.NewClientWithOptions(region, cfg.Config, cfg.Creds)
	if err != nil {
		return nil, err
	}

	timeout := getClientTimeout(d)
	svc.SetReadTimeout(timeout)
	svc.SetConnectTimeout(timeout)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, svc)

	return svc, nil
}

// SLSService returns the client interface for Alicloud Log Service (SLS)
func SLSService(ctx context.Context, d *plugin.QueryData, region string) (sls.ClientInterface, error) {
	if region == "" {
		return nil, fmt.Errorf("region must be provided to initialize the SLS service")
	}

	// have we already created and cached the service?
	serviceCacheKey := fmt.Sprintf("sls-%s", region)
	if cachedData, ok := d.ConnectionManager.Cache.Get(serviceCacheKey); ok {
		return cachedData.(sls.ClientInterface), nil
	}

	// Retrieve cached credentials for authentication
	credCfg, err := getCredentialSessionCached(ctx, d, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve cached credentials: %v", err)
	}
	cfg := credCfg.(*CredentialConfig)

	// Convert to a provider and extract AK/SK/token
	credentialProvider, err := auth.ToCredentialsProvider(cfg.Creds)
	if err != nil {
		return nil, fmt.Errorf("failed to convert credentials to a provider: %v", err)
	}
	profileCred, err := credentialProvider.GetCredentials()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve credentials from the provider: %v", err)
	}

	staticProvider := sls.NewStaticCredentialsProvider(profileCred.AccessKeyId, profileCred.AccessKeySecret, profileCred.SecurityToken)
	endpoint := region + ".log.aliyuncs.com"
	client := sls.CreateNormalInterfaceV2(endpoint, staticProvider)

	// cache the service connection
	d.ConnectionManager.Cache.Set(serviceCacheKey, client)

	return client, nil
}

// GetDefaultRegion returns the default region used
func GetDefaultRegion(connection *plugin.Connection) string {
	// get alicloud config info
	alicloudConfig := GetConfig(connection)

	var regions []string
	var region string

	if alicloudConfig.Regions != nil {
		regions = alicloudConfig.Regions
	}

	if len(regions) > 0 {
		// Set the first region in regions list to be default region
		region = regions[0]
		// check if it is a valid region
		if len(getInvalidRegions([]string{region})) > 0 {
			panic("\n\nConnection config have invalid region: " + region + ". Edit your connection configuration file and then restart Steampipe")
		}
		return region
	}

	if region == "" {
		region = os.Getenv("ALIBABACLOUD_REGION_ID")
		if region == "" {
			region = os.Getenv("ALICLOUD_REGION_ID")
			if region == "" {
				region = os.Getenv("ALICLOUD_REGION")
			}
		}
	}

	if region == "" {
		region = "cn-hangzhou"
	}

	return region
}

// https://github.com/aliyun/aliyun-cli/blob/master/README.md#supported-environment-variables
func getEnvForProfile(_ context.Context, d *plugin.QueryData) (profile string) {
	alicloudConfig := GetConfig(d.Connection)
	if alicloudConfig.Profile != nil {
		profile = *alicloudConfig.Profile
	} else {
		var ok bool
		if profile, ok = os.LookupEnv("ALIBABACLOUD_PROFILE"); !ok {
			if profile, ok = os.LookupEnv("ALIBABA_CLOUD_PROFILE"); !ok {
				if profile, ok = os.LookupEnv("ALICLOUD_PROFILE"); !ok {
					return ""
				}
			}
		}
	}
	return profile
}

func getEnv(_ context.Context, d *plugin.QueryData) (secretKey string, accessKey string, err error) {

	// https://github.com/aliyun/aliyun-cli/blob/master/CHANGELOG.md#3040
	// The CLI order of preference is:
	// 1. ALIBABACLOUD_ACCESS_KEY_ID / ALIBABACLOUD_ACCESS_KEY_SECRET / ALIBABACLOUD_REGION_ID
	// 2. ALICLOUD_ACCESS_KEY_ID / ALICLOUD_ACCESS_KEY_SECRET / ALICLOUD_REGION_ID
	// 3. ACCESS_KEY_ID / ACCESS_KEY_SECRET / REGION
	//
	// The Go SDK and Terraform do:
	// 1. ALICLOUD_ACCESS_KEY / ALICLOUD_SECRET_KEY / ALICLOUD_REGION
	//
	// So, Steampipe will do:
	// 1. ALIBABACLOUD_ACCESS_KEY_ID / ALIBABACLOUD_ACCESS_KEY_SECRET / ALIBABACLOUD_REGION_ID
	// 2. ALICLOUD_ACCESS_KEY_ID / ALICLOUD_ACCESS_KEY_SECRET / ALICLOUD_REGION_ID
	// 3. ALICLOUD_ACCESS_KEY / ALICLOUD_SECRET_KEY / ALICLOUD_REGION

	// get alicloud config info
	alicloudConfig := GetConfig(d.Connection)

	if alicloudConfig.AccessKey != nil {
		accessKey = *alicloudConfig.AccessKey
	} else {
		var ok bool
		if accessKey, ok = os.LookupEnv("ALIBABACLOUD_ACCESS_KEY_ID"); !ok {
			if accessKey, ok = os.LookupEnv("ALICLOUD_ACCESS_KEY_ID"); !ok {
				if accessKey, ok = os.LookupEnv("ALICLOUD_ACCESS_KEY"); !ok {
					panic("\n'access_key' or 'profile' must be set in the connection configuration. Edit your connection configuration file and then restart Steampipe.")
				}
			}
		}
	}

	if alicloudConfig.SecretKey != nil {
		secretKey = *alicloudConfig.SecretKey
	} else {
		var ok bool
		if secretKey, ok = os.LookupEnv("ALIBABACLOUD_ACCESS_KEY_SECRET"); !ok {
			if secretKey, ok = os.LookupEnv("ALICLOUD_ACCESS_KEY_SECRET"); !ok {
				if secretKey, ok = os.LookupEnv("ALICLOUD_SECRET_KEY"); !ok {
					panic("\n'secret_key' or 'profile' must be set in the connection configuration. Edit your connection configuration file and then restart Steampipe.")
				}
			}
		}
	}

	return accessKey, secretKey, nil
}

// Credential configuration
type CredentialConfig struct {
	Creds         auth.Credential
	DefaultRegion string
	Config        *sdk.Config
}

// getClientTimeout returns the configured timeout or 60s default
func getClientTimeout(d *plugin.QueryData) time.Duration {
	// Priority: connection config > environment variable > default (60s)
	config := GetConfig(d.Connection)
	if config.Timeout != nil {
		return time.Duration(*config.Timeout) * time.Second
	}
	if envTimeout := os.Getenv("STEAMPIPE_ALICLOUD_TIMEOUT"); envTimeout != "" {
		if seconds, err := strconv.Atoi(envTimeout); err == nil && seconds > 0 {
			return time.Duration(seconds) * time.Second
		}
	}
	return 60 * time.Second
}

// Get credential from the profile configuration for Alicloud CLI
func getProfileConfigurations(_ context.Context, d *plugin.QueryData) (*CredentialConfig, error) {
	alicloudConfig := GetConfig(d.Connection)
	profile := alicloudConfig.Profile

	cfg, err := getCredentialConfigByProfile(*profile, d)

	if err != nil {
		return nil, err
	}

	return cfg, nil
}

func getCredentialConfigByProfile(profile string, d *plugin.QueryData) (*CredentialConfig, error) {
	defaultRegion := GetDefaultRegion(d.Connection)
	defaultConfig := sdk.NewConfig().WithScheme("HTTPS")
	config := GetConfig(d.Connection)

	if config.AutoRetry != nil {
		defaultConfig = defaultConfig.WithAutoRetry(*config.AutoRetry)
	}
	if config.MaxRetryTime != nil {
		defaultConfig = defaultConfig.WithMaxRetryTime(*config.MaxRetryTime)
	}
	if config.Timeout != nil {
		defaultConfig = defaultConfig.WithTimeout(time.Duration(*config.Timeout) * time.Second)
	} else {
		defaultConfig = defaultConfig.WithTimeout(60 * time.Second)
	}

	// We will get a nil value if the specified profile is not available
	// Or
	// The authentication mode of the profile is not AK | RamRoleArn | StsToken | EcsRamRole As these are the supported type by ALicloud CLI.
	// https://github.com/aliyun/aliyun-cli/blob/master/README.md#configure-authentication-methods

	creds := credentials.NewCLIProfileCredentialsProviderBuilder().WithProfileName(profile).Build()

	return &CredentialConfig{creds, defaultRegion, defaultConfig}, nil
}

var getCredentialSessionCached = plugin.HydrateFunc(getCredentialSessionUncached).Memoize()

func getCredentialSessionUncached(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {

	var connectionCfg *CredentialConfig

	config := GetConfig(d.Connection)
	defaultRegion := GetDefaultRegion(d.Connection)
	defaultConfig := sdk.NewConfig() // initialize with default config

	if config.AutoRetry != nil {
		defaultConfig = defaultConfig.WithAutoRetry(*config.AutoRetry)
	}
	if config.MaxRetryTime != nil {
		defaultConfig = defaultConfig.WithMaxRetryTime(*config.MaxRetryTime)
	}
	if config.Timeout != nil {
		defaultConfig = defaultConfig.WithTimeout(time.Duration(*config.Timeout) * time.Second)
	} else {
		defaultConfig = defaultConfig.WithTimeout(60 * time.Second)
	}

	// Profile based client
	if config.Profile != nil {
		return getProfileConfigurations(ctx, d)
	}

	profileEnv := getEnvForProfile(ctx, d)
	if profileEnv != "" {
		return getCredentialConfigByProfile(profileEnv, d)
	}

	// Access key and Secret Key from environment variable
	accessKey, secretKey, err := getEnv(ctx, d)
	if err != nil {
		return nil, err
	}
	if accessKey != "" && secretKey != "" {
		creds := credentials.NewAccessKeyCredential(accessKey, secretKey)
		connectionCfg = &CredentialConfig{creds, defaultRegion, defaultConfig}
		return connectionCfg, nil
	}

	return nil, nil
}
