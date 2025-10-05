	filename  								Version			Workflow											Scheduled	Versoin	BQ Destination									Description
 	gsc_to_bq.py							REV.4			gsc-to-bq.yml	(No Duplicate)						OK			-		bamtabridsazan__gsc__raw_data					Incremental injection to BQ for last 3 Days
	gsc_to_bq_rev3_full.py					REV.3			gsc-to-bq-full.yml	(No Duplicate)					N/A			-		bamtabridsazan__gsc__raw_data					GSC Full (16Months) fetch
	gsc_to_bq_rev6_fullfetch.py				Rev6.6.14		gsc_fullfetch.yml (No Duplicate)					-			Rev.2	bamtabridsazan__gsc__raw_domain_data_fullfetch			GSC Fullfetch (without searchAppearance)
	gsc_to_bq_searchappearance_fullfetch.py	REV: 6.5.15		gsc_searchappearance_fullfetch.yml (No Duplicate)	-			-		bamtabridsazan__gsc__raw_domain_data_searchappearance	GSC Fullfetch (searchAppearance Only)
	gsc-to-bq-debug-batches.py				-				gsc-debug.yml										N/A			-		-												GSC batch fecth debuging mode (for Full Fetch)
	gsc_to_bq_fullfetch_test.py				-				gsc-to-bq-fullfetch.yml								N/A			-		-												GSC Fullfetch test (without ???)

Above list are the Architectural Files status
