---
name: New verified pipeline
about: I want to create a new verified pipeline
title: "[pipeline name] verified pipeline"
labels: verified pipeline
assignees: ''

---



# Planned Pipeline Info
Please provide the info below when opening the issue:
- **name** of the pipeline to be used by `dlt init` and to be placed in the `pipelines` folder: [e.g., pipedrive]
- **category and description** of the pipeline: [e.g., CRM, loads the relevant data from Pipedrive api]

Fill the data below when writing the spec:

# Use Cases
Please provide descriptions of up to 3 of the most important use cases that users of this pipeline do. Those use cases will be:
- implemented
- reviewed
- demonstrated in the pipeline script
- documented
Use case description is targeted at the developers and people creating test accounts and doing demos.

# Sources / Resources / Endpoints
Define the pipeline’s interface to the user regarding sources and resources:
- Enumerate all the sources with information from which endpoints the data comes from
- Ideally, provide the config arguments to each source (i.e., start dates, report ranges, etc.)
- You can use pseudocode to show how you intend to use the source
- Provide the default write disposition for resources in the source (all append, all replace?)
- In the sources, identify the incremental and merge resources and specify them in a reasonable way (ideally by giving the cursor columns - what is the last value really? primary keys and merge keys).

# Customization
Enumerate everything that goes beyond standard `dlt` building blocks. Suggest the implementation:
- Use of state
- In the code or as an additional transform, filter, or map function
- Ask dlt team for help if it looks like a complex software task

# Test account / test data
- Specify what data you expect in the test dataset. otherwise, refer to use cases
- Specify what kind of test account access you need, including the tool's name, required plan, or features needed

# Implementation tasks
Below you have a proposal for implementation tasks
* [ ] Implement all the described sources, resources, and endpoints
* [ ] Make sure that the Use Cases can be easily executed by the pipeline's user by **providing demonstrations of all the use cases** in the `[pipeline_name]_pipeline.py`
* [ ] Test all the use cases 
* [ ] Test customizations, if there are any, including unit tests for custom code
