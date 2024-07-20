---
{{ card_data }}
language:
- {en}
license: {BSD 3}
license_link: {https://github.com/AQ-AI/openaq-engine/blob/develop/license}
library_name: {openaq-engine}
metrics:
- {mae}
- {mape}
---

# Model Card for Global PM2.5 Air Quality Prediction Model

<!-- Provide a quick summary of what the model is/does. -->

This model predicts PM2.5 air quality at a 1km resolution globally using satellite data, ground sensor data, and other environmental factors.

## Model Details

### Model Description

<!-- Provide a longer summary of what this model is. -->

This model leverages machine learning techniques to predict PM2.5 concentrations globally at a high resolution (1km). It uses a combination of satellite data, meteorological data, and ground sensor data to provide accurate air quality predictions. The model supports various input parameters such as latitude, longitude, and date to generate predictions. It is integrated with a CLI tool and can be deployed on AWS for scalable predictions.

- **Developed by:** AQAI
- **Funded by [optional]:** UNICEF Office for Innovation
- **Shared by [optional]:** AQAI
- **Model type:** Machine Learning Regression Model
- **Language(s) (NLP):** Not Applicable
- **License:** BSD 3-Clause License
- **Finetuned from model [optional]:** Custom implementation

### Model Sources [optional]

<!-- Provide the basic links for the model. -->

- **Repository:** [AQAI GitHub Repository](https://github.com/AQ-AI/openaq-engine)
- **Paper [optional]:** Not Available
- **Demo [optional]:** [Model Deployment](https://github.com/AQ-AI/openaq-engine/releases/tag/v0.1.0)

## Uses

<!-- Address questions around how the model is intended to be used, including the foreseeable users of the model and those affected by the model. -->

### Direct Use

<!-- This section is for the model use without fine-tuning or plugging into a larger ecosystem/app. -->

The model can be directly used to predict PM2.5 concentrations for specific locations and dates by using the provided CLI tool. It is designed for environmental researchers, policy makers, and public health officials to monitor and predict air quality.

### Downstream Use [optional]

<!-- This section is for the model use when fine-tuned for a task, or when plugged into a larger ecosystem/app -->

The model can be integrated into larger environmental monitoring systems, urban planning tools, and public health advisory platforms to provide real-time air quality information and forecasts.

### Out-of-Scope Use

<!-- This section addresses misuse, malicious use, and uses that the model will not work well for. -->

The model should not be used for predicting other pollutants without retraining and validation. It should not be used as the sole source of information for critical health or safety decisions without cross-referencing with other validated data sources.

## Bias, Risks, and Limitations

<!-- This section is meant to convey both technical and sociotechnical limitations. -->

The model relies on the quality and coverage of the input data, which may vary by region and over time. There is a potential for bias in regions with sparse ground sensor coverage. The model's performance may degrade in areas with rapid changes in air quality due to local events like wildfires or industrial accidents.

### Recommendations

<!-- This section is meant to convey recommendations with respect to the bias, risk, and technical limitations. -->

Users (both direct and downstream) should be made aware of the risks, biases, and limitations of the model. It is recommended to validate predictions with local sensor data when available and to use the model as part of a broader air quality monitoring strategy.

## How to Get Started with the Model

Use the code below to get started with the model.

```python
```
## Training Details

### Training Data

<!-- This should link to a Dataset Card, perhaps with a short stub of information on what the training data is all about as well as documentation related to data pre-processing or additional filtering. -->

Training data for our global air pollution models are provided by [OpenAQ](https://openaq.org/), and collected global air quality data from air quality monitoring stations across the globe (sample size 𝑛=1601), measuring
ground-level PM2.5 concentrations from January 2019 to September 2020. The training data includes 169,101,933 PM2.5 measurements from OpenAQ's historic data hosted on AWS S3 and recent measurements are collected from OpenAQ's APIs. The data has been preprocessed to integrate satellite data (AOD, NO2, Night-light), ground sensor data, and meteorological variables.

### Training Procedure

<!-- This relates heavily to the Technical Specifications. Content here should link to that section when it is relevant to the training procedure. -->

#### Preprocessing [optional]

Data preprocessing involves cleaning and integrating data from multiple sources, extracting relevant features, and handling missing values.

#### Training Hyperparameters

- **Training regime:** fp32 precision

#### Speeds, Sizes, Times [optional]

<!-- This section provides information about throughput, start/end time, checkpoint size if relevant, etc. -->

Training times and model sizes vary depending on the dataset and computational resources used.

## Evaluation

<!-- This section describes the evaluation protocols and provides the results. -->

### Testing Data, Factors & Metrics

#### Testing Data

<!-- This should link to a Dataset Card if possible. -->

The testing data includes recent PM2.5 measurements from various global locations. The dataset is divided into training, validation, and testing subsets to evaluate the model's performance.

#### Factors

<!-- These are the things the evaluation is disaggregating by, e.g., subpopulations or domains. -->

Evaluation factors include geographic regions, urban vs. rural areas, and different temporal periods.

#### Metrics

<!-- These are the evaluation metrics being used, ideally with a description of why. -->

Evaluation metrics include F1-Score, Mean Absolute Error (MAE), and Root Mean Squared Error (RMSE).

### Results

The model achieved F1-Scores of 0.66 and 0.71 for Gradient Boosting and Random Forest models, respectively.

#### Summary

The model demonstrates robust performance in predicting PM2.5 concentrations across diverse regions and conditions.

## Model Examination [optional]

<!-- Relevant interpretability work for the model goes here -->

Further interpretability work can include feature importance analysis and visualizations of model predictions against actual observations.

## Environmental Impact

<!-- Total emissions (in grams of CO2eq) and additional considerations, such as electricity usage, go here. Edit the suggested text below accordingly -->

Carbon emissions can be estimated using the [Machine Learning Impact calculator](https://mlco2.github.io/impact#compute) presented in [Lacoste et al. (2019)](https://arxiv.org/abs/1910.09700).

- **Hardware Type:** AWS EC2 Instances
- **Hours used:** Varies by training session
- **Cloud Provider:** AWS
- **Compute Region:** US East (N. Virginia)
- **Carbon Emitted:** Estimated using ML Impact calculator

## Technical Specifications [optional]

### Model Architecture and Objective

The model uses ensemble methods such as Gradient Boosting and Random Forest to predict PM2.5 concentrations. The objective is to provide accurate and high-resolution air quality predictions globally.

### Compute Infrastructure

The model can be deployed on any Linux service or cloud environment. Currently, the model nd infrastructure is trained and deployed on AWS using services including Lambda, S3, Athena, EC2, and EBS.

#### Hardware

AWS EC2 Instances

#### Software

The model uses Python libraries such as scikit-learn, pandas, and numpy for data processing and model training.

## Citation

<!-- If there is a paper or blog post introducing the model, the APA and Bibtex information for that should go in this section. -->
```bibtex
@INPROCEEDINGS{2022AGUFMIN42C0346L,
       author = {{Last}, Christina and {Pramanik}, Prithviraj},
        title = "{Using Open Data to Understand Air Pollution Exposure in Resource-Constrained Environments}",
    booktitle = {AGU Fall Meeting Abstracts},
         year = 2022,
       volume = {2022},
        month = dec,
          eid = {IN42C-0346},
        pages = {IN42C-0346},
       adsurl = {https://ui.adsabs.harvard.edu/abs/2022AGUFMIN42C0346L},
      adsnote = {Provided by the SAO/NASA Astrophysics Data System}
}
```
```bibtex
@article{pramanik2023remote,
  title={Remote Sensing and Machine Learning based models to understand Air Pollution Exposure in Resource-Constrained Environments},
  author={Pramanik, Prithviraj and Last, Christina},
  journal={AGU23},
  year={2023},
  publisher={AGU}
}
```
**BibTeX:**

```bibtex
@misc{aqai2023openaq,
  author = {AQAI},
  title = {Global PM2.5 Air Quality Prediction Model},
  year = {2023},
  howpublished = {\url{https://github.com/AQ-AI/openaq-engine}},
}
