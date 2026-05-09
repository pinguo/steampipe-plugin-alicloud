package alicloud

import (
	"context"
	"slices"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/cas"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableAlicloudCasCertificateOrder(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_cas_certificate_order",
		Description: "Alicloud CAS Certificate Order",
		List: &plugin.ListConfig{
			Hydrate: listCasCertificateOrder,
			Tags:    map[string]string{"service": "cas", "action": "ListUserCertificateOrder"},
		},
		GetMatrixItemFunc: BuildRegionList,
		Columns: []*plugin.Column{
			{
				Name:        "instance_id",
				Description: "The instance ID of the certificate order, used for billing and cost allocation.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "name",
				Description: "The name of the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "certificate_id",
				Description: "The certificate ID. Only available when the certificate has been issued.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "order_id",
				Description: "The order ID.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "aliyun_order_id",
				Description: "The Alibaba Cloud order ID.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "status",
				Description: "The status of the certificate order (PAYED, CHECKING, CHECKED_FAIL, ISSUED, WILLEXPIRED, EXPIRED, NOTACTIVATED, REVOKED).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "cert_type",
				Description: "The type of the certificate (e.g FREE, DV, OV, EV).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "domain",
				Description: "The domain name associated with the certificate order.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "domain_count",
				Description: "The number of domains.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "domain_type",
				Description: "The type of the domain (ONE, MULTIPLE, WILDCARD).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "wild_domain_count",
				Description: "The number of wildcard domain names.",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "product_code",
				Description: "The product code of the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "product_name",
				Description: "The name of the certificate product.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "algorithm",
				Description: "The algorithm of the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "root_brand",
				Description: "The root brand of the certificate (e.g WoSign, CFCA, DigiCert, vTrus).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "source_type",
				Description: "The source type of the order (buy, cpack).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "trustee_status",
				Description: "The hosting status (unTrustee, trustee).",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "common_name",
				Description: "The primary domain name of the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "fingerprint",
				Description: "The certificate fingerprint.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "serial_no",
				Description: "The serial number of the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "sha2",
				Description: "The SHA-2 value of the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "issuer",
				Description: "The certificate authority.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "org_name",
				Description: "The name of the organization that purchases the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "province",
				Description: "The province where the organization is located.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "city",
				Description: "The city where the organization is located.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "country",
				Description: "The country where the organization is located.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "sans",
				Description: "All domain names bound to the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "expired",
				Description: "Indicates whether the certificate has expired.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "upload",
				Description: "Indicates whether the certificate is uploaded by the user.",
				Type:        proto.ColumnType_BOOL,
			},
			{
				Name:        "buy_date",
				Description: "The purchase date (timestamp in milliseconds).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "cert_start_time",
				Description: "The certificate start time (timestamp in milliseconds).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "cert_end_time",
				Description: "The certificate end time (timestamp in milliseconds).",
				Type:        proto.ColumnType_INT,
			},
			{
				Name:        "start_date",
				Description: "The issuance date of the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "end_date",
				Description: "The expiration date of the certificate.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "resource_group_id",
				Description: "The ID of the resource group.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "partner_order_id",
				Description: "The partner order ID.",
				Type:        proto.ColumnType_STRING,
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("Name"),
			},

			// Alicloud standard columns
			{
				Name:        "region",
				Description: ColumnDescriptionRegion,
				Type:        proto.ColumnType_STRING,
				Hydrate:     getCasCertificateOrderRegion,
				Transform:   transform.FromValue(),
			},
			{
				Name:        "account_id",
				Description: ColumnDescriptionAccount,
				Type:        proto.ColumnType_STRING,
				Hydrate:     getCommonColumns,
				Transform:   transform.FromField("AccountID"),
			},
		},
	}
}

//// LIST FUNCTION

func listCasCertificateOrder(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString(matrixKeyRegion)

	// API does not return any error, if the request is made from an unsupported region
	// If the request is made from an unsupported region, it lists all the certificates
	// created in 'cn-hangzhou' region
	// Return nil, if unsupported region (To avoid duplicate entries, when using multi-region configuration)
	if !slices.Contains(supportedRegions, region) {
		return nil, nil
	}

	// Create service connection
	client, err := CasService(ctx, d, region)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_cas_certificate_order.listCasCertificateOrder", "connection_error", err)
		return nil, err
	}

	// Query both CPACK (resource plan orders) and BUY (direct purchase orders)
	// to cover all order types and their different InstanceId formats
	for _, orderType := range []string{"CPACK", "BUY"} {
		request := cas.CreateListUserCertificateOrderRequest()
		request.ShowSize = "50"
		request.CurrentPage = "1"
		request.OrderType = orderType
		request.QueryParams["RegionId"] = region

		count := 0
		for {
			d.WaitForListRateLimit(ctx)
			response, err := client.ListUserCertificateOrder(request)
			if err != nil {
				plugin.Logger(ctx).Error("alicloud_cas_certificate_order.listCasCertificateOrder", "query_error", err, "request", request)
				return nil, err
			}

			for _, i := range response.CertificateOrderList {
				d.StreamListItem(ctx, i)
				if d.RowsRemaining(ctx) == 0 {
					return nil, nil
				}
				count++
			}
			if count >= int(response.TotalCount) {
				break
			}
			request.CurrentPage = requests.NewInteger(int(response.CurrentPage) + 1)
		}
	}

	return nil, nil
}

//// HYDRATE FUNCTIONS

func getCasCertificateOrderRegion(ctx context.Context, d *plugin.QueryData, h *plugin.HydrateData) (interface{}, error) {
	region := d.EqualsQualString(matrixKeyRegion)
	return region, nil
}
