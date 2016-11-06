# redshift-monitor

redshift-monitor allows you to capture, save, and analyze AWS Redshift performance metrics with Tableau Desktop.

It is based on the work done by the fine folks at AWS on the [amazon-redshift-monitoring](https://github.com/awslabs/amazon-redshift-monitoring) project, expect we capture a more metrics, save them in RDS and visualize with Tableau.

### It includes:

* A SQL script to create a metric repository in Amazon Web Services RDS (PostgreSQL)
* An AWS Lambda function (written in Node.js) which collects metrics from Redshift and inserts them into the repository
* A Tableau workbook which connects to the repository and illustrates the use of said metrics. It also documents why these metrics are important

Getting Started
---------------
For an in-depth setup guide, please see the [Setup Wiki](https://github.com/russch/redshift-monitor/wiki/Setup)

To learn about the Tableau workbook and the information you can analyze, read the  [Wiki Home](https://github.com/russch/redshift-monitor/wiki) page.

Is redshift-monitor supported?
---------------
Redshift Monitor is made available AS-IS with no support. Any bugs discovered should be filed in the Redshift Monitor Git issue tracker.

How can I contribute to redshift-monitor?
---------------
Code contributions & improvements by the community are welcomed & encouraged! See the [LICENSE](https://github.com/tableau/redshift-monitor/blob/master/LICENSE) file for current open-source licensing & use information.
