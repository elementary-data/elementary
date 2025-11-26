# Set up Elementary

Building reliable, trustworthy data pipelines shouldn't be painful. Elementary gives you a clear, fast path to understanding your data, monitoring its health, and responding before problems impact your business.

This guide walks you through everything you need to onboard successfully, from connecting your stack to configuration and setting up your first workflows.

With Elementary, you'll quickly be able to:

* Catch issues proactively before they affect consumers  
* Maintain reliable and compliant data across your ecosystem  
* Establish clear ownership and accountability  
* Help everyone discover and understand the data they use

Let's get you set up.

## Elementary's Core Principles

* **Code as the source of truth** - Elementary keeps your code as the system of record. Any change made in the app opens a pull request in your repository, fitting naturally into your CI/CD process and keeping engineers in control. You can work from code, from the UI, or through MCP and everything stays aligned.  
* **Scaling reliability with AI** - Elementary uses AI to help you scale your reliability practices. Ella, our network of AI agents, assists with test creation, metadata enrichment, troubleshooting, and optimization so teams can stay proactive and focus on higher-value work.  
* **Enabling all users** - Elementary is built for the entire organization. Business users get a clear catalog, ownership information, health scores, and AI assistance so everyone can find, understand, and trust the data without depending on engineering.

## Technical Setup

### Setup and integrations

- [ ] [Create Elementary cloud account](https://docs.elementary-data.com/cloud/quickstart) (quick-start guide)  
- [ ] [Install Elementary dbt package](https://docs.elementary-data.com/cloud/onboarding/quickstart-dbt-package)   
      Collect dbt artifacts and enable Elementary's built-in tests (anomaly detection, schema change detection). If you already have the package deployed in the relevant environment you want to monitor, make sure it's up to date to the latest version, and upgrade it if not.  
- [ ] [Connect your data warehouse](https://docs.elementary-data.com/cloud/onboarding/connect-data-warehouse)   
      Elementary reads metadata (dbt artifacts, information schema, query history) to power test results, run history, automated freshness/volume monitors, and column-level lineage.  
- [ ] [Invite team members](https://docs.elementary-data.com/cloud/manage-team)  
- [ ] [Code repository connection (optional)](https://docs.elementary-data.com/cloud/integrations/code-repo/connect-code-repo)   
      Allow users and AI agents to open PRs and manage changes through your CI/CD process.  
- [ ] [Integrate with your BI tool (optional)](https://docs.elementary-data.com/cloud/integrations/bi/connect-bi-tool)   
      Enable column-level lineage and full context for BI assets, including the health of upstream sources and models.  
- [ ] Connect to your external Catalog (Optional)  
      Present Elementary data health context in another catalog, we support [Atlan](https://docs.elementary-data.com/cloud/integrations/governance/atlan).   
- [ ] Non-dbt tables (optional)  
      Elementary has a Python SDK for python-based transformations. If you need Elementary to monitor non-dbt tables, reach out to the Elementary team with a list of relevant schemas / datasets / databases. This is a beta feature, the team will guide you through the next steps.

### Alerts & Incidents

- [ ] Connect messaging app ([Slack](https://docs.elementary-data.com/cloud/integrations/alerts/slack) / [MS Teams](https://docs.elementary-data.com/cloud/integrations/alerts/ms-teams))  
      Receive alerts where your team already works.  
- [ ] Connect [incident management tool](https://docs.elementary-data.com/cloud/integrations/alerts) (optional)  
      We support [Opsgenie](https://docs.elementary-data.com/cloud/integrations/alerts/opsgenie) and [PagerDuty](https://docs.elementary-data.com/cloud/integrations/alerts/pagerduty).  
- [ ] Connect [ticketing system](https://docs.elementary-data.com/cloud/integrations/alerts) (optional)  
      We support [Jira](https://docs.elementary-data.com/cloud/integrations/alerts/jira), [ServiceNow](https://docs.elementary-data.com/cloud/integrations/alerts/servicenow) and [Linear](https://docs.elementary-data.com/cloud/integrations/alerts/linear).

### AI Agents

- [ ] Sign [AI features consent form](https://2cwlc1.share-eu1.hsforms.com/2toUDbhpMRTK0WPmv2az_2g)   
      Elementary AI uses only metadata, not raw or personal data. LLMs run via Amazon Bedrock with no data sharing to third parties. Learn more [here](https://docs.elementary-data.com/cloud/general/ai-privacy-policy).  
- [ ] Elementary to open AI Features

## Advanced Setup

- [ ] [Schedule Elementary syncs](https://docs.elementary-data.com/cloud/guides/sync-scheduling) with your data warehouse.  
- [ ] Contact the Elementary team to enable [Okta SSO](https://docs.elementary-data.com/cloud/integrations/security-and-connectivity/okta), [AWS PrivateLink](https://docs.elementary-data.com/cloud/integrations/security-and-connectivity/aws-privatelink-integration), or [Microsoft Entra ID](https://docs.elementary-data.com/cloud/integrations/security-and-connectivity/ms-entra).  
- [ ] [Configuring Elementary for Development and Production](https://docs.elementary-data.com/cloud/guides/dev-prod-configuration)  
- [ ]  [Manage multiple environments](https://docs.elementary-data.com/cloud/features/multi-env)  
      Use multiple dbt projects and targets (dev, staging, prod) through the multi-env feature.  
      If you wish to monitor multiple environments in a single environment view (including unified lineage), reach out to the Elementary team.  
- [ ] [Collect job information from orchestrator](https://docs.elementary-data.com/cloud/guides/collect-job-data)  
      Elementary can collect metadata about your jobs from the orchestrator you are using, and enrich the Elementary UI with this information.

## Security and permissions

- [ ] Configure [roles and permissions](https://docs.elementary-data.com/cloud/manage-team) in Elementary (optional).  
- [ ] Add SSO authentication with [Okta](https://docs.elementary-data.com/cloud/integrations/security-and-connectivity/okta) or [Azure AD](https://docs.elementary-data.com/cloud/integrations/security-and-connectivity/ms-entra) (optional).  
- [ ] Connect using [AWS private link](https://docs.elementary-data.com/cloud/integrations/security-and-connectivity/aws-privatelink-integration) (optional)   
- [ ] Export [user activity logs](https://docs.elementary-data.com/cloud/features/collaboration-and-communication/user-activity-logs) (optional).

