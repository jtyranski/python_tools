"""
cExtract.py
summary: Scans and extracts all tables from database for a single contractor
created by: Jim Tyranski
created: 08/08/2019
"""

import mysql.connector
import sys, os, argparse
import csv

# get command arguments
parser = argparse.ArgumentParser(description='Scans and extracts all tables from database for a single contractor')
parser.add_argument('masterid', type=int, help='master_id to export')
parser.add_argument('--table', help='run only on single table')

# requires user to provide masterid
args = parser.parse_args()
if args.masterid:
    masterid = args.masterid

# database variables
dbhost = 'localhost'
dbuser = ''
dbpass = ''
dbname = ''

# connect to the database
try:
    mydb = mysql.connector.connect(
        host = dbhost,
        user = dbuser,
        passwd = dbpass,
        db = dbname
    )
except:
    print('Error Connecting to database: ' + dbname)
    os._exit(1)

cursor = mydb.cursor(buffered=True)

# remove any tables with '_copy' appended to them
TABLE_COPY_SQL = """SELECT TABLE_NAME FROM information_schema.TABLES \
    WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '""" + dbname + """' \
        AND TABLE_NAME LIKE '%_copy'
    ORDER BY TABLE_NAME;"""
cursor.execute(TABLE_COPY_SQL)
temp_results=cursor.fetchall()
temp_count = 0;
for temp_row in temp_results:
    temp_name = temp_row[0]
    if '_copy' in temp_name:
        drop_sql = 'DROP TABLE ' + temp_name
        cursor.execute(drop_sql)
        mydb.commit()
        temp_count += 1
print(str(temp_count) + ' copy tables dropped.')

# Get table names
if args.table:
    TABLE_SQL = """SHOW TABLES LIKE '""" + args.table + """';"""
else:
    TABLE_SQL = """SELECT TABLE_NAME FROM information_schema.TABLES \
        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = '""" + dbname + """' \
        ORDER BY TABLE_NAME;"""
cursor.execute(TABLE_SQL)
table_results=cursor.fetchall()

# create empty copies of tables
table_count = 0
for table_row in table_results:
    table_name = table_row[0]
    table_copy = table_name + '_copy'
    create_sql = 'CREATE TABLE IF NOT EXISTS ' + table_copy +' LIKE '+ table_name
    cursor.execute(create_sql)
    mydb.commit()
    table_count += 1

print(str(table_count) + ' copy tables created.')
sqlInsert = []

# populate new tables with only required data (not contractor specific)
sqlInsert.append("""INSERT IGNORE INTO acct_log_copy SELECT * FROM acct_log;""")
sqlInsert.append("""INSERT IGNORE INTO acct_master_copy SELECT * FROM acct_master;""")
sqlInsert.append("""INSERT IGNORE INTO acct_products_copy SELECT * FROM acct_products;""")
sqlInsert.append("""INSERT IGNORE INTO activities_master_copy SELECT * FROM activities_master;""")
sqlInsert.append("""INSERT IGNORE INTO admin_users_copy SELECT * FROM admin_users;""")
sqlInsert.append("""INSERT IGNORE INTO admin_users_copy (user_id, firstname, email, password) \
    VALUES ('1', 'Admin', 'info@email.com', 'NOT REAL');""")
sqlInsert.append("""INSERT IGNORE INTO billing_pricing_copy SELECT * FROM billing_pricing;""")
sqlInsert.append("""INSERT IGNORE INTO billing_pricing2_copy SELECT * FROM billing_pricing2;""")
sqlInsert.append("""INSERT IGNORE INTO calc_defaultMeasurements_copy SELECT * FROM calc_defaultMeasurements;""")
sqlInsert.append("""INSERT IGNORE INTO calc_defaultMeasurementsRes_copy SELECT * FROM calc_defaultMeasurementsRes;""")
sqlInsert.append("""INSERT IGNORE INTO calc_tradeTypes_copy SELECT * FROM calc_tradeTypes;""")
sqlInsert.append("""INSERT IGNORE INTO calc_trades_copy SELECT * FROM calc_trades;""")
sqlInsert.append("""INSERT IGNORE INTO calc_type_copy SELECT * FROM calc_type;""")
sqlInsert.append("""INSERT IGNORE INTO calc_units_copy SELECT * FROM calc_units;""")
sqlInsert.append("""INSERT IGNORE INTO cell_providers_copy SELECT * FROM cell_providers;""")
sqlInsert.append("""INSERT IGNORE INTO def_list_copy SELECT * FROM def_list;""")
sqlInsert.append("""INSERT IGNORE INTO def_list_bidpad_copy SELECT * FROM def_list_bidpad;""")
sqlInsert.append("""INSERT IGNORE INTO demo_users_copy SELECT * FROM demo_users;""")
sqlInsert.append("""INSERT IGNORE INTO direct_accounting_ledger_default_export_templates_copy \
    SELECT * FROM direct_accounting_ledger_default_export_templates;""")
sqlInsert.append("""INSERT IGNORE INTO disciplines_copy SELECT * FROM disciplines;""")
sqlInsert.append("""INSERT IGNORE INTO document_variables_copy SELECT * FROM document_variables;""")
sqlInsert.append("""INSERT IGNORE INTO fcs_connection_copy SELECT * FROM fcs_connection;""")
sqlInsert.append("""INSERT IGNORE INTO global_variables_copy SELECT * FROM global_variables;""")
sqlInsert.append("""INSERT IGNORE INTO homepage_ads_copy SELECT * FROM homepage_ads;""")
sqlInsert.append("""INSERT IGNORE INTO homepage_messages_copy SELECT * FROM homepage_messages;""")
sqlInsert.append("""INSERT IGNORE INTO master_custom_fields_options_copy SELECT * FROM master_custom_fields_options;""")
sqlInsert.append("""INSERT IGNORE INTO master_implementation_copy SELECT * FROM master_implementation;""")
sqlInsert.append("""INSERT IGNORE INTO master_sales_copy SELECT * FROM master_sales;""")
sqlInsert.append("""INSERT IGNORE INTO master_stage_copy SELECT * FROM master_stage;""")
sqlInsert.append("""INSERT IGNORE INTO master_status_copy SELECT * FROM master_status;""")
sqlInsert.append("""INSERT IGNORE INTO modules_copy SELECT * FROM modules;""")
sqlInsert.append("""INSERT IGNORE INTO opportunities_master_copy SELECT * FROM opportunities_master;""")
sqlInsert.append("""INSERT IGNORE INTO prospect_stage_copy SELECT * FROM prospect_stage;""")
sqlInsert.append("""INSERT IGNORE INTO product_profiles_copy SELECT * FROM product_profiles;""")
sqlInsert.append("""INSERT IGNORE INTO property_custom_fields_options_copy SELECT * FROM property_custom_fields_options;""")
sqlInsert.append("""INSERT IGNORE INTO required_fields_copy SELECT * FROM required_fields;""")
sqlInsert.append("""INSERT IGNORE INTO states_copy SELECT * FROM states;""")
sqlInsert.append("""INSERT IGNORE INTO supercali_categories_copy SELECT * FROM supercali_categories;""")
sqlInsert.append("""INSERT IGNORE INTO supercali_groups_copy SELECT * FROM supercali_groups;""")
sqlInsert.append("""INSERT IGNORE INTO supercali_links_copy SELECT * FROM supercali_links;""")
sqlInsert.append("""INSERT IGNORE INTO supercali_modules_copy SELECT * FROM supercali_modules;""")
sqlInsert.append("""INSERT IGNORE INTO supercali_users_copy SELECT * FROM supercali_users;""")
sqlInsert.append("""INSERT IGNORE INTO templates_copy SELECT * FROM templates;""")
sqlInsert.append("""INSERT IGNORE INTO toolbox_cat_copy SELECT * FROM toolbox_cat;""")
sqlInsert.append("""INSERT IGNORE INTO toolbox_master_copy SELECT * FROM toolbox_master;""")
sqlInsert.append("""INSERT IGNORE INTO university_departments_copy SELECT * FROM university_departments;""")
sqlInsert.append("""INSERT IGNORE INTO university_master_classes_copy SELECT * FROM university_master_classes;""")
sqlInsert.append("""INSERT IGNORE INTO university_roles_copy SELECT * FROM university_roles;""")
sqlInsert.append("""INSERT IGNORE INTO zipcodes_copy SELECT * FROM zipcodes;""")
sqlInsert.append("""INSERT IGNORE INTO bidpad_users_copy SELECT * FROM bidpad_users;""")
sqlInsert.append("""INSERT IGNORE INTO direct_formulas_copy SELECT * FROM direct_formulas;""")
sqlInsert.append("""INSERT IGNORE INTO direct_product_materials_copy SELECT * FROM direct_product_materials;""")

# populate new tables with only master_id information
sqlInsert.append("""INSERT IGNORE INTO am_users_copy SELECT * FROM am_users WHERE user_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO bidpad_users_copy SELECT * FROM bidpad_users WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO bidpad_users_profiles_copy SELECT * FROM bidpad_users_profiles \
    WHERE bp_user_id IN (SELECT user_id FROM bidpad_users WHERE master_id=1);""")
sqlInsert.append("""INSERT IGNORE INTO calc_system_copy SELECT * FROM calc_system WHERE master_id=0;""")
sqlInsert.append("""INSERT IGNORE INTO capitalize_copy SELECT * FROM capitalize WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO def_list_master_copy SELECT * FROM def_list_master WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO direct_accounting_ledger_copy SELECT * FROM direct_accounting_ledger WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO direct_systems_copy SELECT * FROM direct_systems WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO master_custom_fields_copy SELECT * FROM master_custom_fields WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO master_list_copy SELECT * FROM master_list WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO modules_to_master_copy SELECT * FROM modules_to_master WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO opportunities_items_copy SELECT * FROM opportunities_items WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO taskmaster_copy SELECT * FROM taskmaster WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO taskmaster_categories_copy SELECT * FROM taskmaster_categories WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO taskmaster_last_action_copy SELECT * FROM taskmaster_last_action WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO toolbox_items_copy SELECT * FROM toolbox_items WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO users_copy SELECT * FROM users WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO prospects_copy SELECT * FROM prospects WHERE \
    master_id=1 AND created_master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO properties_copy SELECT * FROM properties \
    WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=1 \
    AND created_master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opportunities_copy SELECT * FROM opportunities \
    WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE created_master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO activities_copy SELECT * FROM activities \
    WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE created_master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO supercali_events_copy SELECT * FROM supercali_events \
    WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE created_master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO supercali_dates_copy SELECT * FROM supercali_dates \
    WHERE event_id IN (SELECT event_id FROM supercali_events WHERE prospect_id IN \
    (SELECT prospect_id FROM prospects WHERE created_master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO notes_copy SELECT * FROM notes WHERE prospect_id IN \
    (SELECT prospect_id FROM prospects WHERE created_master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO contacts_copy SELECT * FROM contacts WHERE prospect_id IN \
    (SELECT prospect_id FROM prospects WHERE created_master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO powercores_copy SELECT * FROM powercores WHERE master_id=1;""")
sqlInsert.append("""INSERT IGNORE INTO warranty_duration_copy SELECT * FROM warranty_duration \
    WHERE master_id=1;""")

# populate new tables with single contractor information (from master_id)
sqlInsert.append("""INSERT IGNORE INTO account_types_copy SELECT * FROM account_types WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO accounting_change_amount_copy SELECT * FROM accounting_change_amount WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO accounting_chase_response_copy SELECT * FROM accounting_chase_response WHERE payment_id IN (SELECT id FROM accounting_payment_header WHERE invoice_id IN (SELECT id FROM accounting_invoice WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO accounting_invoice_detail_copy SELECT * FROM accounting_invoice_detail WHERE invoice_id IN (SELECT id FROM accounting_invoice WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO accounting_payment_header_copy SELECT * FROM accounting_payment_header WHERE invoice_id IN (SELECT id FROM accounting_invoice WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO accounting_invoice_copy SELECT * FROM accounting_invoice WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO accounting_payment_profiles_copy SELECT * FROM accounting_payment_profiles WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO acct_clientSumm_copy SELECT * FROM acct_clientSumm WHERE clientID=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO acct_productsClient_copy SELECT * FROM acct_productsClient WHERE clientID=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO acct_transactions_copy SELECT * FROM acct_transactions WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO activities_copy SELECT * FROM activities WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO activities_items_copy SELECT * FROM activities_items WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO activities_met_options_copy SELECT * FROM activities_met_options WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO activities_repeat_copy SELECT * FROM activities_repeat WHERE act_id IN (SELECT act_id FROM activities WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_divisions_copy SELECT * FROM am_divisions WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO am_forgot_password_copy SELECT * FROM am_forgot_password WHERE user_id IN (SELECT user_id FROM am_users WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_copy SELECT * FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_custom_fields_copy SELECT * FROM am_leakcheck_custom_fields WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_custom_fields_options_copy SELECT * FROM am_leakcheck_custom_fields_options WHERE custom_id IN (SELECT custom_id FROM am_leakcheck_custom_fields WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_custom_fields_values_copy SELECT * FROM am_leakcheck_custom_fields_values WHERE custom_id IN (SELECT custom_id FROM am_leakcheck_custom_fields WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_extras_copy SELECT * FROM am_leakcheck_extras WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_invoices_copy SELECT * FROM am_leakcheck_invoices WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_materials_copy SELECT * FROM am_leakcheck_materials WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_othercost_copy SELECT * FROM am_leakcheck_othercost WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_payments_copy SELECT * FROM am_leakcheck_payments WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_photos_copy SELECT * FROM am_leakcheck_photos WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_problems_copy SELECT * FROM am_leakcheck_problems WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_resourcelog_copy SELECT * FROM am_leakcheck_resourcelog WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_safety_copy SELECT * FROM am_leakcheck_safety WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_safety_photos_copy SELECT * FROM am_leakcheck_safety_photos WHERE leak_safety_id IN (SELECT leak_safety_id FROM am_leakcheck_safety WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_time_copy SELECT * FROM am_leakcheck_time WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_leakcheck_users_copy SELECT * FROM am_leakcheck_users WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO am_logfile_copy SELECT * FROM am_logfile WHERE user_id IN (SELECT user_id FROM am_users WHERE prospect_id IN (SELECT prospect_id FROM  prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO am_user_logins_copy SELECT * FROM am_user_logins WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO am_users_copy SELECT * FROM am_users WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO asset_management_copy SELECT * FROM asset_management WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO assist_acct_copy SELECT * FROM assist_acct WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO assist_master_copy SELECT * FROM assist_master WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO assist_replacements_copy SELECT * FROM assist_replacements WHERE estimate_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO attachment_library_copy SELECT * FROM attachment_library WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO bidbin_prospects_copy SELECT * FROM bidbin_prospects WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO bidbin_to_prospects_copy SELECT * FROM bidbin_to_prospects WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO bidpad_users_profiles_copy SELECT * FROM bidpad_users_profiles WHERE bp_user_id IN (SELECT user_id FROM bidpad_users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO billing_statistics_copy SELECT * FROM billing_statistics WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO budget_snapshot_copy SELECT * FROM budget_snapshot WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO calc_clientDefaults_copy SELECT * FROM calc_clientDefaults WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO calc_components_copy SELECT * FROM calc_components WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO calc_defaultMeasurements_master_copy SELECT * FROM calc_defaultMeasurements_master WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO calc_extras_copy SELECT * FROM calc_extras WHERE clientID=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO calc_logging_copy SELECT * FROM calc_logging WHERE clientID=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO calc_masterCore_copy SELECT * FROM calc_masterCore WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO calc_materials_copy SELECT * FROM calc_materials WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO calc_project_copy SELECT * FROM calc_project WHERE userID IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO calc_projectFormulas_copy SELECT * FROM calc_projectFormulas WHERE projectID IN (SELECT id FROM calc_project WHERE userID IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO calc_projectMaterials_copy SELECT * FROM calc_projectMaterials WHERE projectID IN (SELECT id FROM calc_project WHERE userID IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO calc_projectMaterialsCust_copy SELECT * FROM calc_projectMaterialsCust WHERE projectID IN (SELECT id FROM calc_project WHERE userID IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO calc_projectMeasurements_copy SELECT * FROM calc_projectMeasurements WHERE projectID IN (SELECT id FROM calc_project WHERE userID IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO calc_system_copy SELECT * FROM calc_system WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO calc_template_copy SELECT * FROM calc_template WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO calc_templateFormulas_copy SELECT * FROM calc_templateFormulas WHERE templateID IN (SELECT id FROM calc_template WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO calc_templateMaterials_copy SELECT * FROM calc_templateMaterials WHERE templateID IN (SELECT id FROM calc_template WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO calc_templateMeasurements_copy SELECT * FROM calc_templateMeasurements WHERE templateID IN (SELECT id FROM calc_template WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO calc_typeOrder_copy SELECT * FROM calc_typeOrder WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO capitalize_copy SELECT * FROM capitalize WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO check_latlng_copy SELECT * FROM check_latlng WHERE (prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)) OR (property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO contacts_copy SELECT * FROM contacts WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO contract_docs_copy SELECT * FROM contract_docs WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO custom_reports_copy SELECT * FROM custom_reports WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO custom_smtp_copy SELECT * FROM custom_smtp WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO custom_smtp_categories_copy SELECT * FROM custom_smtp_categories WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO def_list_categories_copy SELECT * FROM def_list_categories WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO def_list_master_copy SELECT * FROM def_list_master WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_accounting_ledger_copy SELECT * FROM direct_accounting_ledger WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_dmsi_order_log_copy SELECT * FROM direct_dmsi_order_log WHERE material_request_id IN (SELECT id FROM material_requests WHERE contractor_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO direct_formulas_copy SELECT * FROM direct_formulas WHERE template_id IN (SELECT template_id FROM direct_templates WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO direct_internal_notes_copy SELECT * FROM direct_internal_notes WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO direct_materials_categories_copy SELECT * FROM direct_materials_categories WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_materials_custom_copy SELECT * FROM direct_materials_custom WHERE material_id IN (SELECT material_id FROM direct_materials WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO direct_materials_systems_copy SELECT * FROM direct_materials_systems WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_product_materials_copy SELECT * FROM direct_product_materials WHERE material_id IN (SELECT material_id FROM direct_materials WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO direct_systems_copy SELECT * FROM direct_systems WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_systems_custom_copy SELECT * FROM direct_systems_custom WHERE contractor_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_templates_documents_copy SELECT * FROM direct_templates_documents WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO discipline_to_master_copy SELECT * FROM discipline_to_master WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO document_proposal_fcs_copy SELECT * FROM document_proposal_fcs WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO document_queue_copy SELECT * FROM document_queue WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO document_template_copy SELECT * FROM document_template WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO document_template_temp_copy SELECT * FROM document_template_temp WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO drawings_copy SELECT * FROM drawings WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO email_q_copy SELECT * FROM email_q WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO email_verify_copy SELECT * FROM email_verify WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO estimates_copy SELECT * FROM estimates WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO fcs_connection_copy SELECT * FROM fcs_connection WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO forum_logins_copy SELECT * FROM forum_logins WHERE userID IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO gc_properties_copy SELECT * FROM gc_properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO gc_prospects_copy SELECT * FROM gc_prospects WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO geocode_copy SELECT * FROM geocode WHERE (prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=1927)) OR (property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO groups_copy SELECT * FROM groups WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO import_errors_contact_copy SELECT * FROM import_errors_contact WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO import_errors_dispatches_copy SELECT * FROM import_errors_dispatches WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO import_errors_production_copy SELECT * FROM import_errors_production WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO import_errors_property_copy SELECT * FROM import_errors_property WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO import_errors_section_copy SELECT * FROM import_errors_section WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO import_log_copy SELECT * FROM import_log WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO invoice_types_copy SELECT * FROM invoice_types WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO log_delete_copy SELECT * FROM log_delete WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO log_delete_opm_copy SELECT * FROM log_delete_opm WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO log_delete_sd_copy SELECT * FROM log_delete_sd WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO logout_log_copy SELECT * FROM logout_log WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_billing_copy SELECT * FROM master_billing WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_custom_fields_copy SELECT * FROM master_custom_fields WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_custom_fields_options_copy SELECT * FROM master_custom_fields_options WHERE custom_id IN (SELECT custom_id FROM master_custom_fields WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO master_custom_fields_values_copy SELECT * FROM master_custom_fields_values WHERE custom_id IN (SELECT custom_id FROM master_custom_fields WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO master_list_copy SELECT * FROM master_list WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_list_stats_copy SELECT * FROM master_list_stats WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_notes_copy SELECT * FROM master_notes WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_quickbooks_copy SELECT * FROM master_quickbooks WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_quickbooks_other_copy SELECT * FROM master_quickbooks_other WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_term_copy SELECT * FROM master_term WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_totals_copy SELECT * FROM master_totals WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_totals_companies_copy SELECT * FROM master_totals_companies WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_totals_estimates_copy SELECT * FROM master_totals_estimates WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_totals_monthlies_copy SELECT * FROM master_totals_monthlies WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_totals_properties_copy SELECT * FROM master_totals_properties WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO master_usage_copy SELECT * FROM master_usage WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO material_list_copy SELECT * FROM material_list WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO material_list_categories_copy SELECT * FROM material_list_categories WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO material_to_group_copy SELECT * FROM material_to_group WHERE group_id IN (SELECT id FROM groups WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO modules_to_master_copy SELECT * FROM modules_to_master WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO notes_copy SELECT * FROM notes WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_copy SELECT * FROM opm WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO opm_custom_fields_copy SELECT * FROM opm_custom_fields WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO opm_custom_fields_options_copy SELECT * FROM opm_custom_fields_options WHERE custom_id IN (SELECT custom_id FROM opm_custom_fields WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_custom_fields_values_copy SELECT * FROM opm_custom_fields_values WHERE custom_id IN (SELECT custom_id FROM opm_custom_fields WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_email_core_not_copy SELECT * FROM opm_email_core_not WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_email_extra_copy SELECT * FROM opm_email_extra WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_email_portal_not_copy SELECT * FROM opm_email_portal_not WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_entry_copy SELECT * FROM opm_entry WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_entry_photos_copy SELECT * FROM opm_entry_photos WHERE opm_entry_id IN (SELECT opm_entry_id FROM opm_entry WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO opm_entry_videos_copy SELECT * FROM opm_entry_videos WHERE opm_entry_id IN (SELECT opm_entry_id FROM opm_entry WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO opm_invoice_copy SELECT * FROM opm_invoice WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_invoice_extras_copy SELECT * FROM opm_invoice_extras WHERE invoice_id IN (SELECT invoice_id FROM opm_invoice WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO opm_invoice_values_copy SELECT * FROM opm_invoice_values WHERE invoice_id IN (SELECT invoice_id FROM opm_invoice WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO opm_notes_copy SELECT * FROM opm_notes WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_safety_copy SELECT * FROM opm_safety WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_safety_photos_copy SELECT * FROM opm_safety_photos WHERE opm_safety_id IN (SELECT opm_safety_id FROM opm_safety WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO opm_to_sections_copy SELECT * FROM opm_to_sections WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opm_to_users_copy SELECT * FROM opm_to_users WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opportunities_copy SELECT * FROM opportunities WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opportunities_items_copy SELECT * FROM opportunities_items WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO prod_schedule_values_copy SELECT * FROM prod_schedule_values WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO prod_schedule_values_master_copy SELECT * FROM prod_schedule_values_master WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO properties_copy SELECT * FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO properties_images_copy SELECT * FROM properties_images WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO properties_to_account_manager_copy SELECT * FROM properties_to_account_manager WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO properties_to_terms_copy SELECT * FROM properties_to_terms WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO property_custom_fields_copy SELECT * FROM property_custom_fields WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO property_custom_fields_values_copy SELECT * FROM property_custom_fields_values WHERE custom_id IN (SELECT custom_id FROM property_custom_fields WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO property_safety_copy SELECT * FROM property_safety WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO property_safety_photos_copy SELECT * FROM property_safety_photos WHERE property_safety_id IN (SELECT property_safety_id FROM property_safety WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO proposal_to_attachment_copy SELECT * FROM proposal_to_attachment WHERE proposal_id IN (SELECT proposal_id FROM proposals WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO proposal_to_cloud_copy SELECT * FROM proposal_to_cloud WHERE proposal_id IN (SELECT proposal_id FROM proposals WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO proposals_copy SELECT * FROM proposals WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO prospect_notes_copy SELECT * FROM prospect_notes WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO prospects_copy SELECT * FROM prospects WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO prospects_addresses_copy SELECT * FROM prospects_addresses WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO prospects_licenses_copy SELECT * FROM prospects_licenses WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO prospects_resources_copy SELECT * FROM prospects_resources WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO prospects_to_account_manager_copy SELECT * FROM prospects_to_account_manager WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO prospects_to_vendors_copy SELECT * FROM prospects_to_vendors WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO purchase_orders_copy SELECT * FROM purchase_orders WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO purchase_orders_items_copy SELECT * FROM purchase_orders_items WHERE po_id IN (SELECT po_id FROM purchase_orders WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO recommendations_copy SELECT * FROM recommendations WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO report_salesbyclient_copy SELECT * FROM report_salesbyclient WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO required_fields_to_master_copy SELECT * FROM required_fields_to_master WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO roof_report_pdf_copy SELECT * FROM roof_report_pdf WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO rtm_agreements_copy SELECT * FROM rtm_agreements WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO rtm_reports_copy SELECT * FROM rtm_reports WHERE leak_id IN (SELECT leak_id FROM am_leakcheck WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO scope_library_copy SELECT * FROM scope_library WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO sections_copy SELECT * FROM sections WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO sections_coords_copy SELECT * FROM sections_coords WHERE section_id IN (SELECT section_id FROM sections WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO sections_def_copy SELECT * FROM sections_def WHERE section_id IN (SELECT section_id FROM sections WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO sections_interiorphotos_copy SELECT * FROM sections_interiorphotos WHERE section_id IN (SELECT section_id FROM sections WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO sections_photos_copy SELECT * FROM sections_photos WHERE section_id IN (SELECT section_id FROM sections WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO sections_testcuts_copy SELECT * FROM sections_testcuts WHERE section_id IN (SELECT section_id FROM sections WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO sections_videos_copy SELECT * FROM sections_videos WHERE section_id IN (SELECT section_id FROM sections WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO service_maint_agreement_copy SELECT * FROM service_maint_agreement WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO sessions_copy SELECT * FROM sessions WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO sessions_core_copy SELECT * FROM sessions_core WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO sessions_map_copy SELECT * FROM sessions_map WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO signature_delete_copy SELECT * FROM signature_delete WHERE filename LIKE '".""" + str(masterid) + """."_%';""")
sqlInsert.append("""INSERT IGNORE INTO skymeasure_requests_copy SELECT * FROM skymeasure_requests WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO skymeasure_callback_log_copy SELECT * FROM skymeasure_callback_log WHERE order_id IN (SELECT order_id FROM skymeasure_requests WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO slidetracker_copy SELECT * FROM slidetracker WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO supercali_dates_copy SELECT * FROM supercali_dates WHERE event_id IN (SELECT event_id FROM supercali_events WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO supercali_events_copy SELECT * FROM supercali_events WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO takeoff_projects_copy SELECT * FROM takeoff_projects WHERE projectid IN (SELECT project_id FROM estimates WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO taskmaster_copy SELECT * FROM taskmaster WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO taskmaster_last_action_copy SELECT * FROM taskmaster_last_action WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO taskmaster_user_settings_copy SELECT * FROM taskmaster_user_settings WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO tax_groups_copy SELECT * FROM tax_groups WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO timberline_custom_contract_copy SELECT * FROM timberline_custom_contract WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO timecard_activities_copy SELECT * FROM timecard_activities WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO timecard_activities_to_user_copy SELECT * FROM timecard_activities_to_user WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO timecard_certified_class_copy SELECT * FROM timecard_certified_class WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO timecard_entry_copy SELECT * FROM timecard_entry WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO timecard_helpers_copy SELECT * FROM timecard_helpers WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO timecard_entry_helpers_copy SELECT * FROM timecard_entry_helpers WHERE time_id IN (SELECT time_id FROM timecard_helpers WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO timecard_info_boxes_copy SELECT * FROM timecard_info_boxes WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO timecard_jc_activity_copy SELECT * FROM timecard_jc_activity WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO timecard_notes_copy SELECT * FROM timecard_notes WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO timecard_notes_to_user_copy SELECT * FROM timecard_notes_to_user WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO timecard_pay_id_copy SELECT * FROM timecard_pay_id WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO timecard_punch_copy SELECT * FROM timecard_punch WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO tool_active_to_user_copy SELECT * FROM tool_active_to_user WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO tool_available_to_user_copy SELECT * FROM tool_available_to_user WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO toolbox_items_copy SELECT * FROM toolbox_items WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO training_sessions_copy SELECT * FROM training_sessions WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO unsubscribe_admin_copy SELECT * FROM unsubscribe_admin WHERE email IN (SELECT email FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO unsubscribe_opm_copy SELECT * FROM unsubscribe_opm WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO unsubscribe_productionmeeting_copy SELECT * FROM unsubscribe_productionmeeting WHERE opm_id IN (SELECT opm_id FROM opm WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO unsubscribe_servicedispatch_copy SELECT * FROM unsubscribe_servicedispatch WHERE email IN (SELECT email FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO upgrade_survey_copy SELECT * FROM upgrade_survey WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO upgrade_tracking_copy SELECT * FROM upgrade_tracking WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO users_copy SELECT * FROM users WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO users_default_input_copy SELECT * FROM users_default_input WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO users_met_goals_copy SELECT * FROM users_met_goals WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO users_timecard_copy SELECT * FROM users_timecard WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO users_to_crews_copy SELECT * FROM users_to_crews WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO users_to_timecard_info_copy SELECT * FROM users_to_timecard_info WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO videos_embed_copy SELECT * FROM videos_embed WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO assist_prospects_copy SELECT * FROM assist_prospects WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO bidpad_users_copy SELECT * FROM bidpad_users WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_materials_copy SELECT * FROM direct_materials WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_project_items_copy SELECT * FROM direct_project_items WHERE def_id IN (SELECT def_id FROM sections_def WHERE section_id IN (SELECT section_id FROM sections WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """))));""")
sqlInsert.append("""INSERT IGNORE INTO direct_templates_copy SELECT * FROM direct_templates WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_types_copy SELECT * FROM direct_types WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO direct_types_scope_copy SELECT * FROM direct_types_scope WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO dispatch_columns_copy SELECT * FROM dispatch_columns WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO inv_brands_copy SELECT * FROM inv_brands WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO inv_categories_copy SELECT * FROM inv_categories WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO inv_models_copy SELECT * FROM inv_models WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO inv_units_copy SELECT * FROM inv_units WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO login_ip_copy SELECT * FROM login_ip WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO opm_email_log_copy SELECT * FROM opm_email_log WHERE opm_id IN (SELECT opm_id FROM opm WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """));""")
sqlInsert.append("""INSERT IGNORE INTO opportunities_columns_copy SELECT * FROM opportunities_columns WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO opportunities_commission_copy SELECT * FROM opportunities_commission WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO production_workflow_copy SELECT * FROM production_workflow WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO production_workflow_entry_copy SELECT * FROM production_workflow_entry WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO resource_request_copy SELECT * FROM resource_request WHERE request_by_master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO sections_inventory_copy SELECT * FROM sections_inventory WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO takeoff_residential_shapes_copy SELECT * FROM takeoff_residential_shapes WHERE project_id IN (SELECT project_id FROM estimates WHERE property_id IN (SELECT property_id FROM properties WHERE prospect_id IN (SELECT prospect_id FROM prospects WHERE master_id=""" + str(masterid) + """)));""")
sqlInsert.append("""INSERT IGNORE INTO university_classes_copy SELECT * FROM university_classes WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO university_class_records_copy SELECT * FROM university_class_records WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO university_users_copy SELECT * FROM university_users WHERE master_id=""" + str(masterid) + """;""")
sqlInsert.append("""INSERT IGNORE INTO warranty_columns_copy SELECT * FROM warranty_columns WHERE user_id IN (SELECT user_id FROM users WHERE master_id=""" + str(masterid) + """);""")
sqlInsert.append("""INSERT IGNORE INTO warranty_duration_copy SELECT * FROM warranty_duration WHERE master_id=""" + str(masterid) + """;""")

# execute all insert queries
for insert_sql in sqlInsert:
    if not args.table:
        print(insert_sql)
        cursor.execute(insert_sql)
        mydb.commit()
    else:
        if args.table in insert_sql:
            print(insert_sql)
            cursor.execute(insert_sql)
            mydb.commit()

# drop original tables, then rename copied tables to original name
for table_row in table_results:
    table_name = table_row[0]

    drop_sql = 'DROP TABLE ' + table_name
    cursor.execute(drop_sql)
    mydb.commit()

    rename_sql = 'RENAME TABLE ' + table_name + '_copy TO ' + table_name
    cursor.execute(rename_sql)
    mydb.commit()

# create database dump
dumpcmd = "mysqldump -u " + dbuser + " -p" + dbpass + " -h " + dbhost \
    + " " + dbname + " > " + dbname + "_" + str(masterid) + ".sql"
print(dumpcmd)
os.system(dumpcmd)

# compress the database to gzip
gzipcmd = "gzip " + dbname + "_" + str(masterid) + ".sql"
print(gzipcmd)
os.system(gzipcmd)

print("Backup completed.")
