"""Context-enrichment helpers for description_poc_app.py: PII categories,
data domain, related tables, and team ownership, appended to the LLM
prompt via generate_descriptions(..., extra_context=...). All optional —
each function returns empty/None on failure instead of raising, so a
missing permission just means less context, not a crash. See each
function's docstring for its source query and scope.
"""
import logging
import re

logger = logging.getLogger(__name__)

# From the FuelIX model-comparison spreadsheet: mistral-small-3.2-24b and
# gpt-4o-mini-ca-east both "completely break down" on wide tables (~200+
# columns observed failing outright; ~107 columns already needed reruns).
# gemini-2.5-pro-ca held up at all widths tested.
WIDE_TABLE_COLUMN_WARNING = 100
WIDE_TABLE_UNRELIABLE_MODELS = {"mistral-small-3.2-24b", "gpt-4o-mini-ca-east"}

# --- Reference SQL, from tables.sql / columns.sql -----------
DOMAIN_W_CTE = """
select 'ent_sls_chnl' dataset_name,'Sales Channel' domain,'<datahub prefix>_sls_chnl.<table name>' d,'Keeps track of distribution channels and sales activities, sales quotas, sales contests, commission/bonus plans, commissions/bonuses, and maintains groups of individuals that make up the sales force.' domain_desc union all
select 'ent_cust_bill' dataset_name,'Customer Bill' domain,'<datahub prefix>_cust_bill.<table name>' d,'The invoiced statement sent to the customer for services consumed. The form of the invoice can be of various media (paper, eBill)' domain_desc union all
select 'ent_cust_bill_coll' dataset_name,'Customer Bill Collections' domain,'<datahub prefix>_cust_bill_coll.<table name>' d,'Handles credit violations, actions for overdue debts, and facility billing audits' domain_desc union all
select 'ent_cust_cust' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_cust_cust_actvy' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_cust_cust_pref' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_cust_cust_selfserv' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_cust_cust_tos' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'int_cust_cust' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'int_res_cust_cust' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_actvn' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_actvn_rgu' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_actvn_rgu_uat' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_busrgusoln' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_corpsoln' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_supp_prtnr' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_party_credit_profl' dataset_name,'Customer Reference' domain,'<datahub prefix>_cust_cust.<table name>' d,'The reference material that describes a customer’s information at multiple levels (customer, account, product instance). This information includes personal, billing address data, subscriptions to products (eg. price plans) and related serialized equipment' domain_desc union all
select 'ent_cust_intractn' dataset_name,'Customer Interactions','<prefix>_cust_intracn.<table name>' d,'Represents communications with customers, and the translation of customer requests and inquiries into appropriate events.' domain_desc union all
select 'ent_cust_intracn' dataset_name,'Customer Interactions','<prefix>_cust_intracn.<table name>' d,'Represents communications with customers, and the translation of customer requests and inquiries into appropriate events.' domain_desc union all
select 'ent_cust_intractn_ccai' dataset_name,'Customer Interactions','<prefix>_cust_intracn.<table name>' d,'Represents communications with customers, and the translation of customer requests and inquiries into appropriate events.' domain_desc union all
select 'ent_cust_intractn_cntct_evnt' dataset_name,'Customer Interactions','<prefix>_cust_intracn.<table name>' d,'Represents communications with customers, and the translation of customer requests and inquiries into appropriate events.' domain_desc union all
select 'ent_cust_intractn_formbuilder' dataset_name,'Customer Interactions','<prefix>_cust_intracn.<table name>' d,'Represents communications with customers, and the translation of customer requests and inquiries into appropriate events.' domain_desc union all
select 'ent_cust_intractn_survey' dataset_name,'Customer Interactions','<prefix>_cust_intracn.<table name>' d,'Represents communications with customers, and the translation of customer requests and inquiries into appropriate events.' domain_desc union all
select 'int_chat' dataset_name,'Customer Interactions','<prefix>_cust_intracn.<table name>' d,'Represents communications with customers, and the translation of customer requests and inquiries into appropriate events.' domain_desc union all
select 'ent_chat' dataset_name,'Customer Interactions','<prefix>_cust_intracn.<table name>' d,'Represents communications with customers, and the translation of customer requests and inquiries into appropriate events.' domain_desc union all
select 'ent_cust_ord' dataset_name,'Customer Orders' domain,'<datahub prefix>_cust_ord.<table name>' d,'A Product Order is created by or on behalf of a customer to request or configure products.' domain_desc union all
select 'ent_cust_ord_actvy' dataset_name,'Customer Orders' domain,'<datahub prefix>_cust_ord.<table name>' d,'A Product Order is created by or on behalf of a customer to request or configure products.' domain_desc union all
select 'ent_cust_ord_ess' dataset_name,'Customer Orders' domain,'<datahub prefix>_cust_ord.<table name>' d,'A Product Order is created by or on behalf of a customer to request or configure products.' domain_desc union all
select 'ent_cust_request' dataset_name,'Customer Requests' domain,'<datahub prefix>_cust_request.<table name>' d,'Customer initiated requests to change service, billing or other arrangements with TELUS' domain_desc union all
select 'ent_cust_measure' dataset_name,'Customer Measurements and Statistics' domain,'<datahub prefix>_cust_measure.<table name>' d,'Represents the analysis of customer usage patterns, customer profitability statistics and churn and retention statistics.' domain_desc union all
select 'ent_marketing' dataset_name,'Marketing Campaign' domain,'<datahub prefix>_mktg_cpmgn.<table name>' d,'A mechanism by which Marketing Campaigns, Product Promotions, and Product Placements are launched into the marketplace. It describes such mechanisms as the press, radio, trade shows, internet, and so forth. It is also used to advertise other provider Product Promotions and Product Placements.' domain_desc union all
select 'ent_sls_stats' dataset_name,'Sales Statistics' domain,'<datahub prefix>_sls_stats.<table name>' d,'Maintains sales forecasts, new service requirements, customer needs, and customer education, as well as calculating key performance indicators about Sales & Marketing revenue and sales channel performance.' domain_desc union all
select 'ent_sup_prtnr' dataset_name,'Supplier Partner' domain,'<datahub prefix>_sup_prtnr.<table name>' d,'Is the focus for the Supplier/Partner domain. Supplier/Partner represents the enterprise’s knowledge of the Supplier/Partner, their accounts and the relations the Enterprise has with the Supplier/Partner. It also contains all Supplier/Partner agreements and negotiations' domain_desc union all
select 'ent_party_identity' dataset_name,'Identity' domain,'<datahub prefix>_​party_identity.<table name>' d,'The assets that identify individuals (customers, prospects, account managers) that have a relationship to TELUS Communications Inc.' domain_desc union all
select 'ent_party_identity_cntct' dataset_name,'Identity' domain,'<datahub prefix>_​party_identity.<table name>' d,'The assets that identify individuals (customers, prospects, account managers) that have a relationship to TELUS Communications Inc.' domain_desc union all
select 'int_res_party_identity' dataset_name,'Identity' domain,'<datahub prefix>_​party_identity.<table name>' d,'The assets that identify individuals (customers, prospects, account managers) that have a relationship to TELUS Communications Inc.' domain_desc union all
select 'ent_prod_config' dataset_name,'Product Configuration' domain,'<datahub prefix>_prod_config.<table name>' d,'Describes the characteristic dynamic attribution that a product catalogue item can be provisioned to, as well as any mappings to service or resource specifications.' domain_desc union all
select 'ent_prod_offr' dataset_name,'Product Offering' domain,'<datahub prefix>_prod_offr.<table name>' d,'A Product Offer is the representation of product catalogue items (price plans, features, equipment, and rules if present) to the market place for sale, rental or lease for a price. Product offers may represent a simple offering of a single Product Specification or could represent a bundling of one or more other product offers.' domain_desc union all
select 'ent_prod_perf' dataset_name,'Product Performance' domain,'<datahub prefix>_prod_perf.<table name>' d,'Handles product performance goals, the results of end-to-end product performance assessments, and the comparison of assessments against goals. The results may include the identification of potential capacity issues.' domain_desc union all
select 'ent_resrc_config' dataset_name,'Resource Configuration' domain,'<datahub prefix>_​resrc_config.<table name>' d,'The definition of how a Resource operates or functions in terms of Characteristic/Specification(s) and related Resource/Spec(s), as well as a representation of how a Resource operates or functions in terms of characteristics and related Resource(s).' domain_desc union all
select 'ent_resrc_performance' dataset_name,'Resource Performance' domain,'<datahub prefix>_​resrc_performance.<table name>' d,'Collects, correlates, consolidates, and validates various performance statistics and other operational characteristics of Resource entities. It provides a set of entities that can monitor and report on performance. Each of these entities also conducts network performance assessment against planned goals, performs various aspects of trend analysis, including error rate and cause analysis and Resource degradation. Entities in this ABE also define Resource loading, and traffic trend analysis.' domain_desc union all
select 'ent_resrc_performance_device_kpi' dataset_name,'Resource Performance' domain,'<datahub prefix>_​resrc_performance.<table name>' d,'Collects, correlates, consolidates, and validates various performance statistics and other operational characteristics of Resource entities. It provides a set of entities that can monitor and report on performance. Each of these entities also conducts network performance assessment against planned goals, performs various aspects of trend analysis, including error rate and cause analysis and Resource degradation. Entities in this ABE also define Resource loading, and traffic trend analysis.' domain_desc union all
select 'ent_resrc_resrc' dataset_name,'Resource Reference' domain,'<datahub prefix>_resrc_resrc.<table name>' d,'Represent the various aspects of a Resource. This includes four sets of entities that represent: the physical and logical aspects of a Resource; show how to aggregate such resources into aggregate entities that have physical and logical characteristics and behavior; and show how to represent networks, subnetworks, network components, and other related aspects of a network.' domain_desc union all
select 'ent_resrc_spec' dataset_name,'Resource Specification' domain,'<datahub prefix>_resrc_spec.<table name>' d,'Defines the invariant characteristics and behavior of each type of Resource entities. This enables multiple instances to be derived from a single specification entity. In this derivation, each instance will use the invariant characteristics and behavior defined in its associated template.' domain_desc union all
select 'ent_resrc_test' dataset_name,'Resource Test' domain,'<datahub prefix>_resrc_test.<table name>' d,'Tests Physical Resources, Logical Resources, Compound Resources, and Networks. These entities are usually invoked during installation, as a part of trouble diagnosis, or after trouble repair has been completed.' domain_desc union all
select 'ent_srvc_config' dataset_name,'Service Configuration​' domain,'<datahub prefix>_srvc_config.<table name>' d,'The definition of how a Service operates or functions in terms of Characteristic Specification(s) and related Resource Spec(s) and Service Spec(s) as well as a representation of how a Service operates or functions in terms of characteristics and related Resource(s) and Service(s).' domain_desc union all
select 'ent_srvc_performance' dataset_name,'Service Performance' domain,'<datahub prefix>_srvc_performance.<table name>' d,'Collects, correlates, consolidates, and validates various performance statistics and other operational characteristics of customer and resource facing service entities. It provides a set of entities that can monitor and report on performance. Each of these entities also conducts network performance assessment against planned goals, performs various aspects of trend analysis, including error rate and cause analysis and Service degradation. Entities in this ABE also manage the traffic generated by a Service, as well as traffic trend analysis' domain_desc union all
select 'ent_srvc_performance_wls_probe_cp_rtp' dataset_name,'Service Performance' domain,'<datahub prefix>_srvc_performance.<table name>' d,'Collects, correlates, consolidates, and validates various performance statistics and other operational characteristics of customer and resource facing service entities. It provides a set of entities that can monitor and report on performance. Each of these entities also conducts network performance assessment against planned goals, performs various aspects of trend analysis, including error rate and cause analysis and Service degradation. Entities in this ABE also manage the traffic generated by a Service, as well as traffic trend analysis' domain_desc union all
select 'ent_srvc_performance_wls_probe_lsr' dataset_name,'Service Performance' domain,'<datahub prefix>_srvc_performance.<table name>' d,'Collects, correlates, consolidates, and validates various performance statistics and other operational characteristics of customer and resource facing service entities. It provides a set of entities that can monitor and report on performance. Each of these entities also conducts network performance assessment against planned goals, performs various aspects of trend analysis, including error rate and cause analysis and Service degradation. Entities in this ABE also manage the traffic generated by a Service, as well as traffic trend analysis' domain_desc union all
select 'ent_srvc_spec' dataset_name,'Service Specification' domain,'<datahub prefix>_srvc_spec.<table name>' d,'Defines the invariant characteristics and behavior of both types of Service entities. This enables multiple instances to be derived from a single specification entity. In this derivation, each instance will use the invariant characteristics and behavior defined in its associated template. Entities in this ABE focus on adherence to standards, distinguishing features of a Service, dependencies (both physical and logical, as well as on other services), quality, and cost. In general, entities in this ABE enable Services to be bound to Products and run using Resources' domain_desc union all
select 'ent_srvc_srvc' dataset_name,'Service Reference' domain,'<datahub prefix>_srvc_srvc.<table name>' d,'Represents both customer-facing and resource-facing types of services. Entities in this ABE provide different views to examine, analyze, configure, monitor and repair Services of all types. Entities in this ABE are derived from Service Specification entities.' domain_desc union all
select 'ent_srvc_test' dataset_name,'Service Test' domain,'<datahub prefix>_srvc_test.<table name>' d,'Tests customer and resource facing service entities. These entities are usually invoked during installation, as a part of trouble diagnosis or after trouble repair has been completed.' domain_desc union all
select 'ent_trouble_ticket' dataset_name,'Trouble Ticket' domain,'<datahub prefix>_trouble_ticket.<table name>' d,'Any internally or externally (ie customer) initiated request to repair or diagnose service or performance. Customers may initiate an issue with service or billing. Internally technology support may initiate a request to concerning TELUS hardware and applications.' domain_desc union all
select 'ent_usage_rated' dataset_name,'Rated usage' domain,'<datahub prefix>_usage_rated.<table name>' d,'Customer call event records which are mediated and rated for billing. These can be at the detail level or aggregated/summarized' domain_desc union all
select 'ent_usage_rated_tv' dataset_name,'Rated usage' domain,'<datahub prefix>_usage_rated.<table name>' d,'Customer call event records which are mediated and rated for billing. These can be at the detail level or aggregated/summarized' domain_desc union all
select 'ent_usage_unrated' dataset_name,'Unrated Usage' domain,'<datahub prefix>_usage_unrated.<table name>' d,'Customer call event records which are NOT mediated nor rated for billing.' domain_desc union all
select 'ent_usage_unrated_ott' dataset_name,'Unrated Usage' domain,'<datahub prefix>_usage_unrated.<table name>' d,'Customer call event records which are NOT mediated nor rated for billing.' domain_desc union all
select 'ent_workforce' dataset_name,'Workforce Management​' domain,'<datahub prefix>_workforce.<table name>' d,'Represents information associated to the workforce of the enterprise which includes full/part-time employees and contractors. The information will include work schedules, costs, tasks, hierarchy, dependencies and flows' domain_desc union all
select 'ent_common_location' dataset_name,'Location' domain,'<datahub prefix>_common_location.<table name>' d,'Represents the site or position of something, such as a customer’s address, the site equipment where there is a fault and where is the nearest person who could repair the equipment, and so forth. Locations can take the form of coordinates and/or addresses and/or physical representations.' domain_desc union all
select 'ent_common_location_pdc' dataset_name,'Location' domain,'<datahub prefix>_common_location.<table name>' d,'Represents the site or position of something, such as a customer’s address, the site equipment where there is a fault and where is the nearest person who could repair the equipment, and so forth. Locations can take the form of coordinates and/or addresses and/or physical representations.' domain_desc"""

P_TAGS_CTE = """
 select '1006537117176474895' p_tags,'gender' policy_name union all
 select '1143216013069461096' p_tags,'advertising_id' policy_name union all
 select '118935815824139585' p_tags,'team_member_email_address' policy_name union all
 select '1214527753457711736' p_tags,'sms_mms_call_sender' policy_name union all
 select '1415196066345945914' p_tags,'indirect_identifier' policy_name union all
 select '1468718675956119064' p_tags,'health_card_number' policy_name union all
 select '1974268701239764209' p_tags,'banking_information' policy_name union all
 select '2005407955335279314' p_tags,'team_member_business_contact_information' policy_name union all
 select '2082898245559412629' p_tags,'citizenship_number' policy_name union all
 select '2096036089455021247' p_tags,'sms_mms_call_date' policy_name union all
 select '2184664169897358466' p_tags,'referral_information' policy_name union all
 select '2264968161326883898' p_tags,'credit_history_information' policy_name union all
 select '245974072837637401' p_tags,'passport_number' policy_name union all
 select '2499799622227014283' p_tags,'password_reminder' policy_name union all
 select '2527572511402649215' p_tags,'phone_number' policy_name union all
 select '2691802725612173659' p_tags,'street_address' policy_name union all
 select '2906184206123270564' p_tags,'medical_treatment_information' policy_name union all
 select '2914139127405521504' p_tags,'telus_customer_identifier' policy_name union all
 select '3016688225026568762' p_tags,'location' policy_name union all
 select '3072920758842812132' p_tags,'customer_tenure' policy_name union all
 select '3175056636908278160' p_tags,'total_number_of_calls_messages' policy_name union all
 select '3184535704501322710' p_tags,'message_content' policy_name union all
 select '3211781052230735946' p_tags,'internet_usage' policy_name union all
 select '3292500936302160492' p_tags,'team_member_name' policy_name union all
 select '3418053329091841928' p_tags,'salary' policy_name union all
 select '3431392176408678552' p_tags,'customer_employment_information' policy_name union all
 select '3511380797288039344' p_tags,'personal_identifier' policy_name union all
 select '3539322451910183668' p_tags,'customer_device_recordings' policy_name union all
 select '3698208146836101084' p_tags,'ip_address' policy_name union all
 select '40319494658250962' p_tags,'comments_or_memo' policy_name union all
 select '4048383478288038013' p_tags,'team_member_account_username' policy_name union all
 select '4092594293193314251' p_tags,'team_member_corporate_location' policy_name union all
 select '4145097142277877182' p_tags,'customer_account_number' policy_name union all
 select '4166344523492530660' p_tags,'customer_email_address' policy_name union all
 select '4239889151007813510' p_tags,'payroll_info' policy_name union all
 select '4283920693444859827' p_tags,'customer_communication_message' policy_name union all
 select '429871151169900655' p_tags,'mac_address' policy_name union all
 select '459811002539530934' p_tags,'credit_card_number' policy_name union all
 select '4625435525607391553' p_tags,'drivers_license' policy_name union all
 select '4640921118429675635' p_tags,'team_member_attendance' policy_name union all
 select '486922264674860830' p_tags,'password' policy_name union all
 select '4929088276051178611' p_tags,'sexual_orientation' policy_name union all
 select '4938111339976193435' p_tags,'previous_service_provider' policy_name union all
 select '502107759199360893' p_tags,'sms_mms_call_receiver' policy_name union all
 select '506174535084017567' p_tags,'ethnic_origin' policy_name union all
 select '5069368393301447400' p_tags,'religion' policy_name union all
 select '5140501032667813394' p_tags,'device_identifier' policy_name union all
 select '5256758022769833521' p_tags,'duration_of_call' policy_name union all
 select '5278190803940850163' p_tags,'direct_identifier' policy_name union all
 select '549123966996889182' p_tags,'customer_phone_number' policy_name union all
 select '5691084451210127841' p_tags,'date_of_birth' policy_name union all
 select '5912872241141446295' p_tags,'postal_code' policy_name union all
 select '6131029868902366868' p_tags,'social_insurance_number' policy_name union all
 select '6156238726583700336' p_tags,'age' policy_name union all
 select '6393569500387906939' p_tags,'tv_usage' policy_name union all
 select '6410707762300752867' p_tags,'vehicle_identification_number' policy_name union all
 select '6479214341148278695' p_tags,'sms_mms_call_time' policy_name union all
 select '6685032485333625509' p_tags,'inference_about_customer' policy_name union all
 select '6686721322950997298' p_tags,'name' policy_name union all
 select '6794325555523146321' p_tags,'customer_segment' policy_name union all
 select '6838206149689103769' p_tags,'cell_site' policy_name union all
 select '6979634925493767541' p_tags,'customer_preference' policy_name union all
 select '7012984296134411597' p_tags,'seniority_date' policy_name union all
 select '7025600966756142248' p_tags,'customer_complaint' policy_name union all
 select '7091152613254629615' p_tags,'survey_response' policy_name union all
 select '7168774715218534144' p_tags,'financial_info' policy_name union all
 select '7351195420646138002' p_tags,'medical_record' policy_name union all
 select '7358590501324651264' p_tags,'team_member_employment_information' policy_name union all
 select '738878703023100048' p_tags,'biometric' policy_name union all
 select '7594556159118794516' p_tags,'customer_invoice_bill' policy_name union all
 select '7870262057041169057' p_tags,'customer_account_username' policy_name union all
 select '7891043908574412612' p_tags,'customer_call_recording' policy_name union all
 select '7949673661790070055' p_tags,'education_history' policy_name union all
 select '830929669406656048' p_tags,'customer_account_pin' policy_name union all
 select '8324481913154669561' p_tags,'marital_status' policy_name union all
 select '8379888883372275122' p_tags,'fraud_risk_information' policy_name union all
 select '8657644778688690554' p_tags,'health_info_and_benefits' policy_name union all
 select '9126558026838438766' p_tags,'provincial_territorial_identity_number' policy_name 
"""


def resolve_domain(client, dataset_id: str) -> dict | None:
    """{"domain": str, "description": str} for a dataset, via a live query
    against DOMAIN_W_CTE, or None if unrecognized. Falls back to the two
    prefix rules tables.sql applies for datasets not in the explicit list
    (src_* / pub_*). Returns None (not an exception) on query failure."""
    query = "WITH domain_w AS (" + DOMAIN_W_CTE + \
        ") SELECT domain, domain_desc FROM domain_w WHERE dataset_name = @dataset LIMIT 1"
    from google.cloud import bigquery
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("dataset", "STRING", dataset_id)])
    try:
        rows = list(client.query(query, job_config=job_config))
    except Exception as e:
        logger.info("resolve_domain query failed for %s (%s)", dataset_id, type(e).__name__)
        rows = []
    if rows:
        return {"domain": rows[0]["domain"], "description": rows[0]["domain_desc"]}
    if dataset_id.startswith("src"):
        return {"domain": "Raw Data",
               "description": "This dataset contains raw data from a source system."}
    if dataset_id.startswith("pub"):
        return {"domain": "Dedicated Domains",
               "description": "This data is published for a specific business team."}
    return None


def _p_tags_lookup() -> dict:
    """Parses P_TAGS_CTE's literal `select 'id' p_tags,'name' policy_name`
    rows into a plain dict, so the (fast, no-extra-call) local lookup and
    the SQL CTE stay derived from one source of truth instead of two."""
    return dict(re.findall(r"select '(\d+)' p_tags,'([a-z_0-9]+)' policy_name", P_TAGS_CTE))


_PROD_P_TAGS = None  


def _resolve_tag_name(tag_resource_path: str) -> str:
    """Human-readable category for one policy tag's full resource path
    ('projects/.../policyTags/{id}'). Tries the hardcoded prod taxonomy
    list first (fast, no extra call — covers cio-datahub-enterprise-pr-183a
    tags). If the ID isn't in that list — e.g. a *different* taxonomy,
    such as dev's, which uses its own unrelated IDs even for the same
    category — falls back to asking Data Catalog's Policy Tag Manager API
    for the tag's real display_name directly, so this isn't limited to
    whichever project's taxonomy happened to get hardcoded. If even that
    fails (no API access), returns a generic "sensitive (tag not
    resolved)" label rather than silently treating the column as
    untagged — a column carrying an unrecognized tag is still sensitive,
    just of unknown category."""
    global _PROD_P_TAGS
    if _PROD_P_TAGS is None:
        _PROD_P_TAGS = _p_tags_lookup()
    tag_id = tag_resource_path.rstrip("/").split("/")[-1]
    if tag_id in _PROD_P_TAGS:
        return _PROD_P_TAGS[tag_id]
    try:
        from google.cloud import datacatalog_v1
        pt_client = datacatalog_v1.PolicyTagManagerClient()
        return pt_client.get_policy_tag(name=tag_resource_path).display_name
    except Exception as e:
        logger.info("could not resolve policy tag %s via Data Catalog API (%s)",
                   tag_resource_path, type(e).__name__)
        return "sensitive (tag not resolved)"


def fetch_pii_categories(client, project: str, dataset: str, table: str) -> dict:
    """{column_name: [category, ...]} for every column carrying ANY policy
    tag — not just ones matching the hardcoded prod taxonomy list (see
    _resolve_tag_name for how names get resolved across taxonomies).
    Returns {} (not an exception) if the caller lacks access to
    COLUMN_FIELD_PATHS."""
    from google.cloud import bigquery
    query = f"""
    SELECT field_path, ARRAY_AGG(DISTINCT policy_tag) AS tags
    FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` t,
      UNNEST(t.policy_tags) AS policy_tag
    WHERE table_name = @table
    GROUP BY field_path
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("table", "STRING", table)])
    try:
        rows = list(client.query(query, job_config=job_config))
    except Exception as e:
        logger.info("fetch_pii_categories unavailable for %s.%s.%s (%s)",
                   project, dataset, table, type(e).__name__)
        return {}
    return {row["field_path"]: sorted({_resolve_tag_name(t) for t in row["tags"]})
           for row in rows}


def _classify_related(client, query: str, params: list, target: str) -> list:
    """Runs one of the two related-table query variants below and returns
    [{"table", "number_of_queries", "type": "CONFIRMED"|"PROBABLE"}, ...],
    or raises so the caller can decide whether to fall back."""
    from google.cloud import bigquery
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(query, job_config=job_config))
    return [{"table": r["other_table"], "number_of_queries": r["number_of_queries"],
             "type": r["type"]} for r in rows]


def fetch_related_tables(client, project: str, location: str, dataset: str,
                         table: str, *, days: int = 30, min_queries: int = 3,
                         limit: int = 5) -> list:
    """[{"table", "number_of_queries", "type": "CONFIRMED"|"PROBABLE"}, ...] —
    tables queried alongside this one, classified the way datamodels.sql
    does: CONFIRMED means a job referenced exactly these 2 tables together
    (a real join candidate); PROBABLE means the job touched more tables
    too (could be incidental). Scoped to just this one table rather than
    datamodels.sql's full all-pairs scan.

    Tries datamodels.sql's own org-wide job history
    (cio-datahub-work-pr-0be526.datahub_operations.bq_job_by_org) first,
    falling back to `project`'s own INFORMATION_SCHEMA.JOBS_BY_PROJECT if
    that's not accessible — the fallback only sees jobs run under this one
    project. Returns [] rather than raising if neither works."""
    from google.cloud import bigquery
    target = f"{project}.{dataset}.{table}"
    org_wide_query = """
    WITH src AS (
      SELECT job_id,
        CONCAT(referenced_tables.project_id,'.',referenced_tables.dataset_id,
              '.',referenced_tables.table_id) full_table_name
      FROM `cio-datahub-work-pr-0be526`.`datahub_operations`.`bq_job_by_org`,
        UNNEST(referenced_tables) AS referenced_tables
      WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
    ),
    target_jobs AS (SELECT DISTINCT job_id FROM src WHERE full_table_name = @target),
    counts AS (SELECT job_id, COUNT(DISTINCT full_table_name) n FROM src
      WHERE job_id IN (SELECT job_id FROM target_jobs) GROUP BY job_id)
    SELECT s.full_table_name AS other_table, COUNT(DISTINCT s.job_id) number_of_queries,
      CASE WHEN MIN(c.n) = 2 THEN 'CONFIRMED' ELSE 'PROBABLE' END type
    FROM src s JOIN target_jobs t USING (job_id) JOIN counts c USING (job_id)
    WHERE s.full_table_name != @target
    GROUP BY other_table HAVING COUNT(DISTINCT s.job_id) >= @min_queries
    ORDER BY number_of_queries DESC LIMIT @limit
    """
    params = [bigquery.ScalarQueryParameter("days", "INT64", days),
             bigquery.ScalarQueryParameter("target", "STRING", target),
             bigquery.ScalarQueryParameter("min_queries", "INT64", min_queries),
             bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    try:
        return _classify_related(client, org_wide_query, params, target)
    except Exception as e:
        logger.info("fetch_related_tables org-wide unavailable for %s (%s) — "
                   "falling back to project-scoped", target, type(e).__name__)
    project_scoped_query = f"""
    WITH src AS (
      SELECT job_id,
        CONCAT(referenced_tables.project_id,'.',referenced_tables.dataset_id,
              '.',referenced_tables.table_id) full_table_name
      FROM `{project}`.`region-{location}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT,
        UNNEST(referenced_tables) AS referenced_tables
      WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
    ),
    target_jobs AS (SELECT DISTINCT job_id FROM src WHERE full_table_name = @target),
    counts AS (SELECT job_id, COUNT(DISTINCT full_table_name) n FROM src
      WHERE job_id IN (SELECT job_id FROM target_jobs) GROUP BY job_id)
    SELECT s.full_table_name AS other_table, COUNT(DISTINCT s.job_id) number_of_queries,
      CASE WHEN MIN(c.n) = 2 THEN 'CONFIRMED' ELSE 'PROBABLE' END type
    FROM src s JOIN target_jobs t USING (job_id) JOIN counts c USING (job_id)
    WHERE s.full_table_name != @target
    GROUP BY other_table HAVING COUNT(DISTINCT s.job_id) >= @min_queries
    ORDER BY number_of_queries DESC LIMIT @limit
    """
    try:
        return _classify_related(client, project_scoped_query, params, target)
    except Exception as e:
        logger.info("fetch_related_tables unavailable for %s (%s)", target, type(e).__name__)
        return []


_TEAM_QUERY = """
with src as (
select referenced_tables.project_id Project, referenced_tables.dataset_id Dataset_name,
  referenced_tables.table_id table_name, user_email,
  string_agg(distinct j.project_id) job_project_name
from `cio-datahub-work-pr-0be526`.`datahub_operations`.`bq_job_by_org` j,
  unnest(referenced_tables) referenced_tables
where referenced_tables.project_id like 'cio-datahub-ent%'
  and creation_time > timestamp_sub(current_timestamp, INTERVAL 30 DAY)
  and (referenced_tables.dataset_id like 'ent%' or referenced_tables.dataset_id like 'int%'
       or referenced_tables.dataset_id like 'src%' or referenced_tables.dataset_id like 'pub%')
group by referenced_tables.project_id, referenced_tables.dataset_id, referenced_tables.table_id, user_email),
s as (select
  team_member_id, concat(first_nm,' ',family_nm) team_member_nm,
  upper(work_email_address_txt) work_email_address_txt,
  effective_sp_corp_job_catgy_cd, effective_sp_team_member_id, effective_sp_job_title,
  concat(effective_sp_first_nm,' ',effective_sp_family_nm) nm
from `cio-datahub-work-pr-0be526`.`datahub_temp`.`ent_cust_cust_bq_team_member_dim`
where current_ind='Y'),
team as (
select s.work_email_address_txt, s.team_member_nm,
  case when s.effective_sp_corp_job_catgy_cd='MGR' then s.nm
       when s1.effective_sp_corp_job_catgy_cd='MGR' then s1.nm
       when s2.effective_sp_corp_job_catgy_cd='MGR' then s2.nm
       when s3.effective_sp_corp_job_catgy_cd='MGR' then s3.nm
       when s4.effective_sp_corp_job_catgy_cd='MGR' then s4.nm
       when s5.effective_sp_corp_job_catgy_cd='MGR' then s5.nm
       when s6.effective_sp_corp_job_catgy_cd='MGR' then s6.nm end MGR,
  case when s.effective_sp_corp_job_catgy_cd='DIR' then s.nm
       when s1.effective_sp_corp_job_catgy_cd='DIR' then s1.nm
       when s2.effective_sp_corp_job_catgy_cd='DIR' then s2.nm
       when s3.effective_sp_corp_job_catgy_cd='DIR' then s3.nm
       when s4.effective_sp_corp_job_catgy_cd='DIR' then s4.nm
       when s5.effective_sp_corp_job_catgy_cd='DIR' then s5.nm
       when s6.effective_sp_corp_job_catgy_cd='DIR' then s6.nm end DIR,
  case when s.effective_sp_corp_job_catgy_cd='VP' then s.nm
       when s1.effective_sp_corp_job_catgy_cd='VP' then s1.nm
       when s2.effective_sp_corp_job_catgy_cd='VP' then s2.nm
       when s3.effective_sp_corp_job_catgy_cd='VP' then s3.nm
       when s4.effective_sp_corp_job_catgy_cd='VP' then s4.nm
       when s5.effective_sp_corp_job_catgy_cd='VP' then s5.nm
       when s6.effective_sp_corp_job_catgy_cd='VP' then s6.nm end VP
from s left outer join s s1 on (s.effective_sp_team_member_id=s1.team_member_id)
left outer join s s2 on (s1.effective_sp_team_member_id=s2.team_member_id)
left outer join s s3 on (s2.effective_sp_team_member_id=s3.team_member_id)
left outer join s s4 on (s3.effective_sp_team_member_id=s4.team_member_id)
left outer join s s5 on (s4.effective_sp_team_member_id=s5.team_member_id)
left outer join s s6 on (s5.effective_sp_team_member_id=s6.team_member_id)
where s.work_email_address_txt is not null)
select src.Dataset_name, src.table_name, team.team_member_nm, team.MGR, team.DIR, team.VP
from src left outer join team on (upper(trim(src.user_email))=team.work_email_address_txt)
where team.team_member_nm is not null
"""


def fetch_team_ownership_raw(client) -> list:
    """Runs the org-wide businessteams.sql query as-is (scans 30 days of
    every BigQuery job in the organization plus the HR team-member
    dimension) — this is the heavy one. Call this at most once per session
    (cache it in the caller) and filter the result per table with
    team_for_table(). Both bq_job_by_org and the HR team-member dimension
    live in cio-datahub-work-pr-0be526. Returns [] rather than raising if
    the caller lacks access — team context is optional, not required for
    description generation to proceed."""
    try:
        return [dict(row) for row in client.query(_TEAM_QUERY)]
    except Exception as e:
        logger.warning("fetch_team_ownership_raw failed (%s) — team context "
                       "will be unavailable for this session", type(e).__name__)
        return []


def team_for_table(rows: list, dataset: str, table: str) -> dict | None:
    """First matching {team_member_nm, MGR, DIR, VP} for dataset.table from
    fetch_team_ownership_raw()’s output, or None if nobody queried it in
    the lookback window."""
    for r in rows:
        if r.get("Dataset_name") == dataset and r.get("table_name") == table:
            return {k: v for k, v in r.items()
                   if k in ("team_member_nm", "MGR", "DIR", "VP") and v}
    return None


def build_extra_context(*, domain: dict | None, pii: dict, related: list,
                        team: dict | None) -> str:
    """Assembles the free-text block passed to
    generate_descriptions(..., extra_context={table: this}). Only includes
    sections that actually have data — an empty/all-None run of context
    fetches produces just the telecom-industry line, not empty headers."""
    lines = ["\nCOMPANY CONTEXT: TELUS is a Canadian telecommunications "
            "company — interpret column names in that industry context "
            "(e.g. billing, wireless/wireline service, customer accounts)."]
    if domain:
        lines.append(f"\nDATA DOMAIN: {domain['domain']} — {domain['description']}")
    if pii:
        cats = "; ".join(f"{col}: {', '.join(names)}" for col, names in pii.items())
        lines.append("\nKNOWN PII CATEGORIES (from policy tags — treat these "
                     "columns' contents as sensitive personal information; "
                     "do not soften or omit that this column is regulated, "
                     "but never restate example values):\n" + cats)
    if related:
        rel = "; ".join(f"{r['table']} ({r['type']}, {r['number_of_queries']} "
                        f"shared queries)" for r in related)
        lines.append("\nRELATED TABLES (join candidates — CONFIRMED = a job "
                     "referenced only these two tables together, a real "
                     "join candidate; PROBABLE = the job touched other "
                     "tables too):\n" + rel)
    if team:
        who = ", ".join(f"{k}: {v}" for k, v in team.items())
        lines.append("\nOWNING TEAM (most frequent recent querier's "
                     "reporting line — reference only, do not restate "
                     "verbatim in the description):\n" + who)
    return "\n".join(lines)


def fetch_abbreviations_from_bq(client, table_ref: str) -> dict:
    """{full_word: abbreviation} from a BigQuery abbreviation lookup table
    (e.g. cio-datahub-work-pr-0be526.datahub_operations.bq_abbr_list),
    with NO hardcoded column names — the two relevant columns are detected
    at query time:
    1. By name: a column matching abbr/short/acronym is the abbreviation;
       one matching full/long/word/desc/mean/expan is the full form.
    2. If that's ambiguous (not exactly one match on each side), falls
       back to sampling 50 rows and picking whichever of the table's
       string columns has the shorter average length as the abbreviation.
    Returns {} (not an exception) if `table_ref` isn't accessible, or if
    neither heuristic can confidently identify two columns. Shape matches
    dq_core.load_abbreviations() so this can be merged with the local
    abbreviations.csv and passed straight into
    generate_descriptions(abbrev=...)."""
    import re
    from google.cloud import bigquery
    try:
        project, dataset, table = table_ref.split(".")
    except ValueError:
        logger.warning("fetch_abbreviations_from_bq: %r is not "
                       "project.dataset.table", table_ref)
        return {}
    schema_query = f"""
    SELECT column_name, data_type FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = @table ORDER BY ordinal_position
    """
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("table", "STRING", table)])
    try:
        schema = [(r["column_name"], r["data_type"])
                 for r in client.query(schema_query, job_config=job_config)]
    except Exception as e:
        logger.info("fetch_abbreviations_from_bq schema lookup failed for %s (%s)",
                   table_ref, type(e).__name__)
        return {}
    strings = [c for c, dt in schema if dt == "STRING"]
    if len(strings) < 2:
        return {}
    abbr_re = re.compile(r"abbr|short|acronym", re.IGNORECASE)
    full_re = re.compile(r"full|long|word|desc|mean|expan", re.IGNORECASE)
    abbr_matches = [c for c in strings if abbr_re.search(c)]
    full_matches = [c for c in strings if full_re.search(c)]
    if len(abbr_matches) == 1 and len(full_matches) == 1:
        abbr_col, full_col = abbr_matches[0], full_matches[0]
    else:
        # Name-based detection was ambiguous — fall back to average length
        # on a sample: shorter column is the abbreviation.
        cols = ", ".join(f"`{c}`" for c in strings[:2])
        try:
            sample = list(client.query(
                f"SELECT {cols} FROM `{project}.{dataset}.{table}` LIMIT 50"))
        except Exception as e:
            logger.info("fetch_abbreviations_from_bq sample failed for %s (%s)",
                       table_ref, type(e).__name__)
            return {}
        if not sample:
            return {}
        c1, c2 = strings[0], strings[1]
        avg1 = sum(len(r[c1] or "") for r in sample) / len(sample)
        avg2 = sum(len(r[c2] or "") for r in sample) / len(sample)
        abbr_col, full_col = (c1, c2) if avg1 <= avg2 else (c2, c1)
    try:
        rows = list(client.query(
            f"SELECT `{full_col}` AS full_word, `{abbr_col}` AS abbr "
            f"FROM `{project}.{dataset}.{table}`"))
    except Exception as e:
        logger.info("fetch_abbreviations_from_bq full fetch failed for %s (%s)",
                   table_ref, type(e).__name__)
        return {}
    return {(r["full_word"] or "").strip().lower(): (r["abbr"] or "").strip().lower()
           for r in rows if r["full_word"]}