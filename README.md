# BasicStatisticsSpark
Spark Basic Statistics Library for Spark DataFrames

## Description 
Library aims to be the Basic Statistics Library for Spark Data Frames to perform Univariate, Multivariate Analysis of features. 
Which can help in deciding the features for the prediction model by providing a statistical analysis of the features. Only three
of the basic tests has been included in the library as of now which are mainly used for Bi-variate analysis when one feature is 
Continuous and other is Categorical, it helps in deciding the difference between group averages of the values of the continuous feature. 
The analysis tests included are : 

###### ANOVA (Analysis of Variance)
Analysis of variance (ANOVA) is a collection of statistical models and their associated estimation procedures (such as the 
"variation" among and between groups) used to analyze the differences among group means in a sample. ANOVA was developed by 
statistician and evolutionary biologist Ronald Fisher. In the ANOVA setting, the observed variance in a particular variable 
is partitioned into components attributable to different sources of variation. In its simplest form, ANOVA provides a 
statistical test of whether the population means of several groups are equal, and therefore generalizes the t-test to more 
than two groups. ANOVA is useful for comparing (testing) three or more group means for statistical significance. It is 
conceptually similar to multiple two-sample t-tests, but is more conservative (results in less type I error)[1] and is 
therefore suited to a wide range of practical problems.

###### Z-Test
A Z-test is any statistical test for which the distribution of the test statistic under the null hypothesis can be approximated by a normal distribution. Because of the central limit theorem, many test statistics are approximately normally distributed for large samples. 

The term "Z-test" is often used to refer specifically to the one-sample location test comparing the mean of a set of measurements to a given constant. If the observed data X1, ..., Xn are (i) uncorrelated, (ii) have a common mean μ, and (iii) have a common variance σ2, then the sample average X has mean μ and variance σ2 / n. If our null hypothesis is that the mean value of the population is a given number μ0, we can use X −μ0 as a test-statistic, rejecting the null hypothesis if X − μ0 is large.

###### T-Test
A t-test is most commonly applied when the test statistic would follow a normal distribution if the value of a scaling term in the test statistic were known. When the scaling term is unknown and is replaced by an estimate based on the data, the test statistics (under certain conditions) follow a Student's t distribution. The t-test can be used, for example, to determine if two sets of data are significantly different from each other.

A two-sample location test of the null hypothesis such that the means of two populations are equal. All such tests are usually called Student's t-tests, though strictly speaking that name should only be used if the variances of the two populations are also assumed to be equal.

## Usage 
###### ANOVA
`` anova(df, continuousFeatureColumnName, categoricalFeatureColumnName) ``

###### Z-Test
`` zTest(df, continuousFeatureColumnName, categoricalFeatureColumnName) ``

###### T-Test
`` tTest(df, continuousFeatureColumnName, categoricalFeatureColumnName) ``

