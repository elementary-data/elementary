<p align="center">
<img alt="Logo" src="static/headline-git.png"/>
</p>

<p align="center">
<a href="https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg"><img src="https://img.shields.io/badge/join-Slack-orange"/></a>
<img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-brightgreen"/>
<img alt="Downloads" src="https://static.pepy.tech/personalized-badge/elementary-lineage?period=total&units=international_system&left_color=grey&right_color=blue&left_text=Downloads"/>
<img alt="GitHub last commit" src="https://img.shields.io/github/last-commit/elementary-data/elementary-lineage?color=ff69b4"/>
</p>


Our goal is to provide data teams **immediate visibility, detection of data issues, and impact analysis.** 
We focus on **simple effortless setup**, and integrations with the existing stack.

Our first two modules:

* **Data lineage made simple, reliable, and automated**
Tracing the actual upstream & downstream dependencies in the data warehouse, without any implementation efforts or security risks.
* **Detection of changes in source tables**
Get Slack alerts on changes in your source tables, detect breaking changes and discover new data you can leverage. 




:star: If you like what we are building, support us with a <a href="https://github.com/elementary-data/elementary-lineage/stargazers"><img src="static/star_github.png" width="40"/></a> :star:

<img src="static/elementary_demo.gif" width="750"/>

## Sandbox

Try out our new [live demo here](https://www.elementary-data.com/live-demo).

## Quick start

```bash
pip install elementary-lineage

# The tool is named edr (Elementary Data Reliability),
# run it to validate the installation:
edr --help
```

We need a connection profile in `profiles.yml`. [Here is a template](static/profiles.yml) to create one.   
For further instructions and guides to start using the different modules, go to our [quickstart page](https://docs.elementary-data.com/quickstart). 

After you created a [connection profile](https://docs.elementary-data.com/guides/connection-profile), generate a lineage graph:
```
# Creates a lineage graph from queries executed between 7 days ago and current time, 
# for the database named 'my_db'

edr lineage -db my_db
```

Afte you [configure sources to monitor](https://docs.elementary-data.com/guides/connection-profile), execute it using:
```
edr monitor
```

## Documentation & Demo

Our full documentation is [available here](https://docs.elementary-data.com/). 
We also created a short [demo video](https://docs.elementary-data.com/demo). 



## Features

**Data lineage**
* **Lineage visualization**: Visual map of data flow and dependencies in the data warehouse. 
* **Dataset status**: Present data about freshness and volume on the lineage graph.
* **Accuracy**: Reflects the actual state in the DWH based on logs.
* **Plug-and-play**: No need for code changes.
* **Graph filters**: Filter the graph by dataset, dates, direction, and depth. 

**Source tables monitoring**
* **Slack notifications**.
* **Detect deletions:** columns and tables that were removed.
* **Detect data type** changes.
* **Detect new data:** columns and tables that were added.


**You can impact our next features in this [roadmap](https://github.com/elementary-data/elementary-lineage/projects/1)** by voting :+1: to issues and opening new ones.

Our plan is to eventually build a data observability platform that is open, transparent and powered by the community. 
A solution that data teams could easily integrate into their workflows, detect data incidents and prevent them from even happening in the first place.

We are working on **Dataset monitoring**, **Lineage history**, **Column level lineage**, **Full lineage**, and more.


## Community & Support

For additional information and help, you can use one of these channels:

* [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) \(Live chat with the team, support, discussions, etc.\)
* [GitHub issues](https://github.com/elementary-data/elementary-lineage/issues) \(Bug reports, feature requests)
* [Roadmap](https://github.com/elementary-data/elementary-lineage/projects/1) \(Vote for features and add your inputs)
* [Twitter](https://twitter.com/ElementaryData) \(Updates on new releases and stuff)

## **Integrations**

* [x] **Snowflake** ![](static/snowflake-16.png)
* [x] **BigQuery**  ![](static/bigquery-16.png) - Lineage only
* [ ] **Redshift**  ![](static/redshift-16.png) 

Ask us for integrations on [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) or as a [GitHub issue](https://github.com/elementary-data/elementary-lineage/issues/new).

## **License**

Elementary lineage is licensed under Apache License 2.0. See the [LICENSE](https://github.com/elementary-data/elementary-lineage/blob/master/LICENSE) file for licensing information.
