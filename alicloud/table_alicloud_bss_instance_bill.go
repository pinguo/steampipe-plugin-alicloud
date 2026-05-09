package alicloud

import (
	"context"
	"fmt"
	"time"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/alibaba-cloud-sdk-go/services/bssopenapi"

	"github.com/turbot/steampipe-plugin-sdk/v5/grpc/proto"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin/transform"
)

//// TABLE DEFINITION

func tableAlicloudBssInstanceBill(ctx context.Context) *plugin.Table {
	return &plugin.Table{
		Name:        "alicloud_bss_instance_bill",
		Description: "Alicloud BSS Instance Bill",
		List: &plugin.ListConfig{
			Hydrate: listBssInstanceBill,
			Tags:    map[string]string{"service": "bssopenapi", "action": "DescribeInstanceBill"},
			KeyColumns: plugin.KeyColumnSlice{
				{Name: "billing_cycle", Require: plugin.Optional},
				{Name: "billing_year", Require: plugin.Optional},
				{Name: "product_code", Require: plugin.Optional},
				{Name: "instance_id", Require: plugin.Optional},
				{Name: "subscription_type", Require: plugin.Optional},
				{Name: "billing_date", Require: plugin.Optional},
				{Name: "granularity", Require: plugin.Optional},
			},
		},
		Columns: []*plugin.Column{
			// Primary identifiers
			{
				Name:        "instance_id",
				Description: "The ID of the instance.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("InstanceID"),
			},
			{
				Name:        "product_code",
				Description: "The code of the service.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "product_name",
				Description: "The name of the service.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "product_type",
				Description: "The type of the service.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "product_detail",
				Description: "The details of the service.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "subscription_type",
				Description: "The billing method (Subscription, PayAsYouGo).",
				Type:        proto.ColumnType_STRING,
			},

			// Billing info
			{
				Name:        "billing_cycle",
				Description: "The billing cycle in YYYY-MM format. Defaults to all months of the current year if not specified.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "billing_year",
				Description: "The billing year in YYYY format. When specified, queries all months (Jan-Dec) of that year. For the current year, queries up to the current month.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("billing_year"),
			},
			{
				Name:        "billing_date",
				Description: "The billing date in YYYY-MM-DD format.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "billing_type",
				Description: "The billing type.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "billing_item",
				Description: "The billing item.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "billing_item_code",
				Description: "The code of the billing item.",
				Type:        proto.ColumnType_STRING,
			},

			// Cost fields
			{
				Name:        "pretax_gross_amount",
				Description: "The pretax gross amount.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "pretax_amount",
				Description: "The pretax amount.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "payment_amount",
				Description: "The amount paid.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "outstanding_amount",
				Description: "The outstanding amount.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "deducted_by_coupons",
				Description: "The amount deducted by coupons.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "deducted_by_cash_coupons",
				Description: "The amount deducted by cash coupons.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "deducted_by_prepaid_card",
				Description: "The amount deducted by prepaid card.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "deducted_by_resource_package",
				Description: "The amount deducted by resource package.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "invoice_discount",
				Description: "The invoice discount.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "adjust_amount",
				Description: "The adjustment amount.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "cash_amount",
				Description: "The cash amount.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "currency",
				Description: "The currency.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "tax",
				Description: "The tax amount.",
				Type:        proto.ColumnType_DOUBLE,
			},
			{
				Name:        "after_tax_amount",
				Description: "The after-tax amount.",
				Type:        proto.ColumnType_DOUBLE,
			},

			// Instance info
			{
				Name:        "instance_spec",
				Description: "The specification of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "instance_config",
				Description: "The configuration of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "internet_ip",
				Description: "The public IP address.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("InternetIP"),
			},
			{
				Name:        "intranet_ip",
				Description: "The private IP address.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("IntranetIP"),
			},
			{
				Name:        "region_no",
				Description: "The region ID.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "zone",
				Description: "The zone.",
				Type:        proto.ColumnType_STRING,
			},

			// Usage info
			{
				Name:        "usage",
				Description: "The usage.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "usage_unit",
				Description: "The unit of usage.",
				Type:        proto.ColumnType_STRING,
			},

			// Time info
			{
				Name:        "service_period",
				Description: "The service period.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "service_period_unit",
				Description: "The unit of the service period.",
				Type:        proto.ColumnType_STRING,
			},

			// Owner info
			{
				Name:        "owner_id",
				Description: "The ID of the resource owner.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("OwnerID"),
			},
			{
				Name:        "owner_name",
				Description: "The name of the resource owner.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "bill_account_id",
				Description: "The ID of the billing account.",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("BillAccountID"),
			},
			{
				Name:        "bill_account_name",
				Description: "The name of the billing account.",
				Type:        proto.ColumnType_STRING,
			},

			// Other
			{
				Name:        "commodity_code",
				Description: "The commodity code.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "resource_group",
				Description: "The resource group.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "tags_src",
				Description: "A list of tags attached with the resource.",
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Tag").Transform(bssSourceTags),
			},
			{
				Name:        "tags",
				Description: ColumnDescriptionTags,
				Type:        proto.ColumnType_JSON,
				Transform:   transform.FromField("Tag").Transform(bssTagsToMap),
			},
			{
				Name:        "cost_unit",
				Description: "The cost center.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "nick_name",
				Description: "The nickname of the instance.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "pip_code",
				Description: "The PIP code.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "list_price",
				Description: "The list price.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "list_price_unit",
				Description: "The unit of the list price.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "biz_type",
				Description: "The business type.",
				Type:        proto.ColumnType_STRING,
			},
			{
				Name:        "granularity",
				Description: "The granularity of the bill (MONTHLY, DAILY).",
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromQual("granularity"),
			},

			// Steampipe standard columns
			{
				Name:        "title",
				Description: ColumnDescriptionTitle,
				Type:        proto.ColumnType_STRING,
				Transform:   transform.FromField("InstanceID"),
			},

			// Alicloud standard columns
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

// getBillingCycles returns the list of billing cycles to query.
// Priority: billing_cycle > billing_year > default (current year).
func getBillingCycles(d *plugin.QueryData) []string {
	// 1. Specific month
	if cycle := d.EqualsQualString("billing_cycle"); cycle != "" {
		return []string{cycle}
	}

	// 2. Specific year
	if yearStr := d.EqualsQualString("billing_year"); yearStr != "" {
		return buildYearCycles(yearStr)
	}

	// 3. Default: current year up to current month
	now := time.Now()
	return buildYearCycles(fmt.Sprintf("%d", now.Year()))
}

// buildYearCycles generates billing cycles for a given year (YYYY).
// For the current year, only generates up to the current month.
// For past years, generates all 12 months.
func buildYearCycles(yearStr string) []string {
	now := time.Now()
	currentYear := now.Year()
	currentMonth := int(now.Month())

	year := 0
	fmt.Sscanf(yearStr, "%d", &year)
	if year == 0 {
		year = currentYear
	}

	maxMonth := 12
	if year == currentYear {
		maxMonth = currentMonth
	}

	cycles := make([]string, 0, maxMonth)
	for m := 1; m <= maxMonth; m++ {
		cycles = append(cycles, fmt.Sprintf("%d-%02d", year, m))
	}
	return cycles
}

func listBssInstanceBill(ctx context.Context, d *plugin.QueryData, _ *plugin.HydrateData) (interface{}, error) {
	client, err := BssOpenApiService(ctx, d)
	if err != nil {
		plugin.Logger(ctx).Error("alicloud_bss_instance_bill.listBssInstanceBill", "connection_error", err)
		return nil, err
	}

	billingCycles := getBillingCycles(d)

	for _, billingCycle := range billingCycles {
		request := bssopenapi.CreateDescribeInstanceBillRequest()
		request.Scheme = "https"
		request.BillingCycle = billingCycle

		// Optional filters
		if d.EqualsQualString("product_code") != "" {
			request.ProductCode = d.EqualsQualString("product_code")
		}
		if d.EqualsQualString("instance_id") != "" {
			request.InstanceID = d.EqualsQualString("instance_id")
		}
		if d.EqualsQualString("subscription_type") != "" {
			request.SubscriptionType = d.EqualsQualString("subscription_type")
		}
		if d.EqualsQualString("billing_date") != "" {
			request.BillingDate = d.EqualsQualString("billing_date")
		}
		if d.EqualsQualString("granularity") != "" {
			request.Granularity = d.EqualsQualString("granularity")
		}

		request.MaxResults = requests.NewInteger(300)

		for {
			d.WaitForListRateLimit(ctx)
			response, err := client.DescribeInstanceBill(request)
			if err != nil {
				plugin.Logger(ctx).Error("alicloud_bss_instance_bill.listBssInstanceBill", "query_error", err, "request", request)
				return nil, err
			}

			if !response.Success {
				plugin.Logger(ctx).Error("alicloud_bss_instance_bill.listBssInstanceBill", "api_error", response.Code, "message", response.Message)
				break
			}

			for _, item := range response.Data.Items {
				d.StreamListItem(ctx, item)
				if d.RowsRemaining(ctx) == 0 {
					return nil, nil
				}
			}

			if response.Data.NextToken == "" {
				break
			}
			request.NextToken = response.Data.NextToken
		}
	}

	return nil, nil
}
