<p align="center">
<img alt="Logo" src="static/headline-git.png"/>
</p>

<p align="center">
<a href="https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg"><img src="https://img.shields.io/badge/join-Slack-orange"/></a>
<img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-brightgreen"/>
<img alt="Downloads" src="https://static.pepy.tech/personalized-badge/elementary-lineage?period=total&units=international_system&left_color=grey&right_color=blue&left_text=Downloads"/>
<img alt="GitHub last commit" src="https://img.shields.io/github/last-commit/elementary-data/elementary-lineage?color=ff69b4"/>
</p>


Elementary was built out of the need to **effortlessly and immediately gain visibility** into the data stack,
starting with tracing the actual upstream & downstream dependencies in the data warehouse, 
without any implementation efforts, security risks or compromises on accuracy. 

**Data lineage made simple, reliable, and automated.**




:star: If you like what we are building, support us with a <a href="https://github.com/elementary-data/elementary-lineage/stargazers"><img src="static/star_github.png" width="40"/></a> :star:

<img src="static/elementary_demo.gif" width="750"/>

## Sandbox

Try out our new [live demo here](https://www.elementary-data.com/live-demo).

## Quick start

```bash
pip install elementary-lineage

# The tool is named edl (Elementary Data Lineage),
# run it to validate the installation:
edl --help
```

We need a connection file in a simple YAML called `profiles.yml`. [Here is a template](static/profiles.yml) to create a Snowflake / BigQuery one.   
For further instructions go to our [quickstart page](https://docs.elementary-data.com/quickstart#requirements). 

If you use ![](static/dbt-16.png)[dbt](https://www.getdbt.com/), you can start right away by running this command with the path to your `profiles.yml` and the relevant profile name:

```bash
edl -d ~/.dbt -p <profile_name>
```

## Documentation & Demo

Our full documentation is [available here](https://docs.elementary-data.com/). 
We also created a short [demo video](https://docs.elementary-data.com/demo). 



## Features

* **Lineage visualization:** Visual map of data flow and dependencies in the data warehouse. 
* **Dataset status:** Present data about freshness and volume on the lineage graph.
* **Accuracy:** Reflects the actual state in the DWH based on logs.
* **Plug-and-play:** No need for code changes.
* **Graph filters:** Filter the graph by dataset, dates, direction, and depth. 



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
* [x] **BigQuery**  ![](static/bigquery-16.png) 
* [ ] **Redshift**  ![](static/redshift-16.png) 

Ask us for integrations on [Slack](https://join.slack.com/t/elementary-community/shared_invite/zt-uehfrq2f-zXeVTtXrjYRbdE_V6xq4Rg) or as a [GitHub issue](https://github.com/elementary-data/elementary-lineage/issues/new).

## **License**

Elementary lineage is licensed under Apache License 2.0. See the [LICENSE](https://github.com/elementary-data/elementary-lineage/blob/master/LICENSE) file for licensing information.
