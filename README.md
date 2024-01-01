# TO DO
- Test web scraping with:
    - https://github.com/serve-and-volley/atp-world-tour-tennis-data
- Change Git repository to public
- I'll be using https://awsacademy.instructure.com/courses/61569/modules/items/5435475 AWS Academy Learner Lab


# Environment setup
1. `conda env create -f environment.yml`
2. `conda activate big-data-env`


# Technology stack
## Programming languages
- Python
- LaTeX
- Bash

## Big Data
- Apache Spark through PySpark

## Software tools
- AWS CLI version 2
- Git
- MacTeX
- Miniconda

## Development Environment
- Visual Studio Code



# Data
Tennis data is obtained from two repositories:
- For men's professional tennis: [https://github.com/JeffSackmann/tennis_atp.git](https://github.com/JeffSackmann/tennis_atp.git)
- For women's professional tennis: [https://github.com/JeffSackmann/tennis_wta.git](https://github.com/JeffSackmann/tennis_wta.git)

Above two repositories are set up as Git submodules within this repository.

All data from these sources is attributed to Jeff Sackmann and is licensed under the terms of Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International License.


# Using AWS CLI
1) `aws configure`
2) Upload CSV files to S3 with `bash src/aws/upload_to_s3.sh`

