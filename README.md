adrenalina
==========

a selection of my coding examples, covering web development and automation

automation scripts:
------------------

metrics_aggr_cacti

    Perl, DBIx::Class

    scripts for fetching, storing and processing cacti data in MySQL database that run as cron jobs
    stored metrics statistics used for creating a heatmap as part of a web-based administrative tool
    
pool_metrics_aggr-graphite.pl

    Perl, DBI

    script for fetching and processing graphite data for VM servers in a pool and storing the aggregates in a separate, 
    per pool, Whisper file

xxx_report

    Perl, DBI
    
    script for emailing daily and weekly colour coded reports about servers that were out of service and for what reason
    
web_exaples:
-----------

MetricsDashboard
    
    Perl, Catalyst, DBIC, Template::Toolkit, jQuery
    
    shows a heatmap of daily/weekly/monthly respective metrics of various devices (switches, firewalls, servers).
    Provides comment making feature. Allows collapsing to improve readability and increase clarity of the results.

API_App

    Perl, Catalyst, DBIC, Template::Toolkit, jQuery
    
    example of a RESTful controller and a helper library

helper_SmokeTest.t

    unit tests using mocking
