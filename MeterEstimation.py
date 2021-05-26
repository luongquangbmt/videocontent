from pyspark.sql import SparkSession

import pyspark.sql.functions as functions
from pyspark.sql.functions import udf, lit, col, when, abs, format_number, corr, variance, mean, rank, unix_timestamp, datediff
from pyspark.sql.functions import to_date, month, collect_list
from pyspark.sql.window import Window

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, Matrices, MatrixUDT

from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
from pyspark.sql.types import FloatType
from spark_sklearn.util import createLocalSparkSession
from spark_sklearn.keyed_models import KeyedEstimator

from datetime import datetime, timedelta

import numpy as np
import sys

#create a writable spark session
spark = (SparkSession
                .builder
                .appName('MeterEstimation')
                .enableHiveSupport()
                .getOrCreate())
spark.sparkContext.setLogLevel("OFF")

def getCoef(model):
    """return models coefficients"""
    return str(model.coef_[0])
      
def getIntercept(model):
    """return models intercept"""
    return str(model.intercept_)

def keyed_mean(df, keyCol, dataCol):
    "Calculate mean of keyed rows"
    partitionWindow = Window.partitionBy(df[keyCol])
    return functions.mean(df[dataCol]).over(partitionWindow)

def keyed_de_mean(df, keyCol, dataCol):
    "Calculate difference around mean of keyed rows"
    partitionWindow = Window.partitionBy(df[keyCol])
    return df[dataCol] - keyed_mean(df, keyCol, dataCol)
    
median_udf = udf(lambda x: float(np.median(x)), FloatType())

class BillingEstimator:
    
    def __init__(self, model_created_date):
        self.name = "Billing Estimate Model"
        self.author = "Author: IBM/EGD"
        if type(model_created_date) is str:
            self.date = datetime.strptime(model_created_date, '%Y%m%d')
        else:
            self.date = model_created_date  #all data prior to this date will not be used for modelling
        print(self.name)
        print(self.author)
        print("Model Created date:", self.date)
        print("-"*10)

    def create_new_meter_table(self, mr_table, te107_df, model_date):
        "create new final meter read table that prun all uncessary rows and columns, derive consumption"

        print("1.   CREATE NEW METER READ TABLE WITH CONSUMPTION")
        print("1.1 Filter rows")
        
        mr_df = spark.sql("select * from cis_restrict_tal.meter_read \
                            WHERE istablart in ('01', '02', '06', '07', 'C1', 'LA', 'LC', 'M3', 'S1', 'V1', 'W1') \
                            AND ablstat in ('1','4') \
                            AND temp_area in ('THOROLD', 'PETERBOR','METROGTA', 'OTTAWA', 'BARRIE') \
                            AND tariftyp in ('1_GD', '6_GD')")        
        
        print("1.2 Derive consumption from reads")    
        #Calculate consumption by subtracting the current read to the preceding read
        windowSpec = Window.partitionBy(mr_df.anlage).orderBy(mr_df.adat)
        mr_df = mr_df.withColumn('periodic_consumption', mr_df.v_zwstand - (functions.lag(mr_df.v_zwstand, 1,).over(windowSpec)))
        mr_df = mr_df.withColumn("from_date", (functions.lag("adat", 1,0).over(windowSpec)))
        mr_df = mr_df.withColumn('to_date', mr_df.adat)
        #Adding read_month columns (cam be moved to data engineering)
        mr_df = mr_df.withColumn('read_month', month(to_date('to_date','yyyyMMdd')).cast('int'))
           
        print("1.3 Remove the consumption calculated from the first read of new meter after meter changes or dial restart")
        partitionWindow = Window.partitionBy(mr_df.anlage, mr_df.equnr).orderBy(mr_df.adat)
        rankTest = rank().over(partitionWindow)
        mr_df = mr_df.withColumn("Rank", rankTest)
        mr_df = mr_df.where(mr_df.Rank != '1').drop('Rank')

        print("1.4 Create periodic_days and daily_consumption columns")
        mr_df = mr_df.withColumn('adat_dt', to_date(unix_timestamp('adat','yyyyMMdd')\
                                 .cast('timestamp')))
        mr_df = mr_df.withColumn('from_date_dt',to_date(unix_timestamp('from_date','yyyyMMdd')\
                                 .cast('timestamp')))
        mr_df = mr_df.withColumn('periodic_days', datediff(mr_df.adat_dt,mr_df.from_date_dt))\
                                  .filter(col('periodic_days') > 0)
        mr_df = mr_df.withColumn('daily_consumption', mr_df.periodic_consumption/mr_df.periodic_days)
        mr_df = mr_df.filter(col('daily_consumption') >=0 )
        mr_df = mr_df.drop('adat_dt','from_date_dt')
        
        print("1.5 Join meter read data and te107 to create periodic_dd, avera_daily_dd, consumption_per_dd")
        print("1.6 Create periodic_dd, aver_daily_dd, consumption_per_dd")
         
        df1 = mr_df.alias('df1')
        df2 = te107_df.alias('df2')
        
        df2_temp = df2['gradtdat', 'sumgtz', 'temp_area']
        #print("# rows before joining", df1.count())
        df = df1.join(df2_temp.withColumnRenamed('gradtdat','from_date')\
                      .withColumnRenamed('sumgtz','sumgtz_from_date'), \
                      ['from_date','temp_area'])

        df2_temp = df2['gradtdat', 'sumgtz', 'temp_area']
        df = df.join(df2_temp.withColumnRenamed('gradtdat','to_date')\
                     .withColumnRenamed('sumgtz','sumgtz_to_date'), \
                     ['to_date','temp_area'])

        df = df.withColumn('periodic_dd', (df.sumgtz_to_date-df.sumgtz_from_date))
        df = df.withColumn('aver_daily_dd', df.periodic_dd/(df.periodic_days))
        df = df.withColumn('consumption_per_dd', (df.periodic_consumption)/(df.periodic_dd))
       
        df = df.filter((col('daily_consumption') >= 0.0) & \
                       #(col('daily_consumption') < 500.0) & \
                       (col('consumption_per_dd') >= 0.0) & \
                       #(col('consumption_per_dd') < 100.0) & \
                       (col('periodic_days') > 0.0) & \
                       (col('periodic_days') < 365))
        
        df = df.withColumn('to_date',to_date(unix_timestamp('to_date','yyyyMMdd')\
                                 .cast('timestamp')))
        df = df.filter(col('to_date') > datetime.strptime(str(20100101), '%Y%m%d'))

        #Calculate median summer consumption and remove premises with no summer data point
        summer_df = df.filter((col('aver_daily_dd') <= 5))
        four_year_past = (model_date-timedelta(days=365*4))#.strptime('%Y%m%d')
        summer_df = summer_df.filter((col('to_date') >= four_year_past)).select('anlage', 'daily_consumption').dropDuplicates()
        partitionWindow = Window.partitionBy(summer_df.anlage)
        summer_df = summer_df.withColumn("cons_list", collect_list("daily_consumption").over(partitionWindow))
        summer_df = summer_df.withColumn("median_summer_cons", median_udf("cons_list"))
        count_read_sum = functions.count(col('anlage')).over(partitionWindow)
        summer_df = summer_df.withColumn("count_read_summer", count_read_sum.cast('int'))
        summer_df = summer_df.withColumn("summer_cons", when((col('median_summer_cons') >= 0), col('median_summer_cons')).otherwise(lit('0')))
        summer_df = summer_df.select('anlage','count_read_summer',col('summer_cons').cast('double')).dropDuplicates()
        
        df = df.join(summer_df, 'anlage', how='left').dropDuplicates()
        df = df.filter(col('count_read_summer') > 0)
        
        #Calculate median summer consumption and remove premises with no summer data point
        winter_df = df.filter((col('aver_daily_dd') > 5) & (col('daily_consumption') > 0))
        two_year_past = str((model_date-timedelta(days=365*2)).strftime('%Y%m%d'))
        two_year_past = (model_date-timedelta(days=365*2))
        winter_df = winter_df.filter((col('to_date') >= two_year_past)).select('anlage', 'daily_consumption').dropDuplicates()
        
        partitionWindow = Window.partitionBy(winter_df.anlage)
        winter_df = winter_df.withColumn("cons_list", collect_list("daily_consumption").over(partitionWindow))
        winter_df = winter_df.withColumn("median_winter_cons", median_udf("cons_list"))      
        count_read_win = functions.count(col('anlage')).over(partitionWindow)
        winter_df = winter_df.withColumn("count_read_winter", count_read_win.cast('int'))
        winter_df = winter_df.select('anlage','count_read_winter',col('median_winter_cons').cast('double')).dropDuplicates()
        
        df = df.join(winter_df, 'anlage', how='left').dropDuplicates()  
        df = df.filter(col('count_read_winter') > 0)
 
        print("1.7 Create impala table meter_read_w_dd")
        df.withColumn('to_date',to_date(unix_timestamp('to_date','yyyy-MM-dd').cast('timestamp')))\
          .withColumn("to_date", col('to_date').cast('string')).createOrReplaceTempView("df")
        
        
        spark.sql("drop table IF EXISTS cis_restrict_tal.meter_read_w_dd")
        spark.sql("create table cis_restrict_tal.meter_read_w_dd as select * from df")
        print("new table cis_restrict_tal.meter_read_w_dd was created")
        
        print("Create model using data up to this date: ", model_date)
        spark.catalog.clearCache()
        return df
      
    def load_mr_df(self, cons_df, model_date):    
        print("2. LOADING ACTUAL-ACTUAL CONSUMPTION")
        
        #Remove premises having less than 1 summer read and 1 winter read
        partitionWindow = Window.partitionBy(cons_df.anlage)
                
        #Select only column for modelling
        col_for_model= ['anlage', 'to_date', 'read_month', 'daily_consumption',  'periodic_days', 'periodic_dd','aver_daily_dd','periodic_consumption', \
'consumption_per_dd','median_winter_cons',"summer_cons"]
        cons_df_for_model = cons_df[col_for_model].dropDuplicates().na.drop(subset=['anlage', 'daily_consumption'])
        cons_df_for_model = cons_df_for_model.filter(col('to_date') <= model_date)     
        '''
        cons_df_for_model.createOrReplaceTempView("cons_df_for_model")
        spark.sql("drop table IF EXISTS cis_restrict_tal.cons_df_for_model")
        spark.sql("create table cis_restrict_tal.cons_df_for_model as SELECT * from cons_df_for_model")

        cons_df.createOrReplaceTempView("cons_df")
        spark.sql("drop table IF EXISTS cis_restrict_tal.cons_df")
        spark.sql("create table cis_restrict_tal.cons_df as SELECT * from cons_df")
        '''     
        spark.catalog.clearCache()
        return cons_df, cons_df_for_model


    def fit_model(self, cons_df_for_model, model_date):

        print("3. FITTING DATA")
        print("3.1 Fitting data with linear regression models")
        
        #Build a keyed dataframe [key, feature, target]
        #Assemble feature vector, which can be more than one variable
       
        _winter_df = cons_df_for_model.select('anlage','to_date','median_winter_cons','summer_cons','aver_daily_dd','daily_consumption')
        _winter_df =_winter_df.filter((col('aver_daily_dd') >= 5) & (col('daily_consumption') > 0))
        partitionWindow = Window.partitionBy(_winter_df.anlage).orderBy(_winter_df.daily_consumption)
        percentile = functions.percent_rank().over(partitionWindow)
        _winter_df = _winter_df.withColumn('percentile_q', percentile)
        _winter_df = _winter_df.filter((col('percentile_q') <= 0.95) & (col('percentile_q') >= 0.05))
        _winter_df = _winter_df.drop('percentile_q')
        
        #Remove premises having less than 3 reads in winter
        partitionWindow = Window.partitionBy(_winter_df.anlage)
        
        count_read = functions.count(col('anlage')).over(partitionWindow)
        _winter_df = _winter_df.withColumn("count_read_winfit", count_read.cast('int'))
        
        vectorAssembler = VectorAssembler(inputCols = \
                                              ['aver_daily_dd'], outputCol = 'features')
        ln_model_input_df = vectorAssembler.transform(_winter_df)
        km = KeyedEstimator(sklearnEstimator=LinearRegression(), keyCols=['anlage'], yCol="daily_consumption",  outputCol='coefficient2').fit(ln_model_input_df)
        #Get the Coef and Intercept from the scikit-learn models
        coef_Col = udf(getCoef)("estimator").cast('double').alias("coefficient1")
        intercept_Col = udf(getIntercept)("estimator").cast('double').alias("intercept")

        #Create the fitted result dataframe
        _results_df = km.keyedModels.select("anlage", intercept_Col, coef_Col)
          
        final_expected_models = cons_df_for_model['anlage', 'summer_cons','median_winter_cons'].dropDuplicates()        
        final_expected_models = final_expected_models.join(_winter_df['anlage','count_read_winfit'], ['anlage'], how='left').dropDuplicates()
        
        _fin_results_df = final_expected_models.join(_results_df.dropDuplicates(), ['anlage'], how='left').dropDuplicates()
        
        _fin_results_df = _fin_results_df.withColumn("summer_baseload", when((col('summer_cons') >= 0), col('summer_cons')).otherwise(lit('0'))).dropDuplicates()
        _fin_results_df = _fin_results_df.withColumn("degree_day_factor", when((col('coefficient1') >= 0), col('coefficient1')).otherwise(lit('0'))).dropDuplicates()
        _fin_results_df = _fin_results_df.withColumn("winter_baseload", col('intercept') +5*col('coefficient1'))
        _fin_results_df = _fin_results_df.withColumn("winter_baseload", when((col('winter_baseload') >= 0.001), col('winter_baseload')).otherwise(lit('0.001'))).dropDuplicates()        
        _fin_results_df = _fin_results_df.withColumn("winter_baseload", when((col('coefficient1') > 0), col("winter_baseload")).otherwise(col("median_winter_cons"))).dropDuplicates()
        _fin_results_df = _fin_results_df.withColumn("degree_day_factor", when(((col('count_read_winfit') > 3) & (col('degree_day_factor') < 50)), col("degree_day_factor")).otherwise(lit("0"))).dropDuplicates()
        _fin_results_df = _fin_results_df.withColumn("winter_baseload", when(((col('count_read_winfit') > 3) & (col('degree_day_factor') < 50)), col("winter_baseload")).otherwise(col("median_winter_cons"))).dropDuplicates()
                
        for out_col in _fin_results_df.columns:
            if out_col != 'anlage':
                _fin_results_df = _fin_results_df.withColumn(out_col, col(out_col).cast('decimal(10,3)'))          
            
        spark.catalog.clearCache()
        
        print("3.2 Calculate accuracy")
        
        mr_w_est_w = _winter_df.join(_fin_results_df.dropDuplicates(),'anlage', how='left')
        
        s_cons = col('summer_baseload')
        w_cons = col('winter_baseload') + col('degree_day_factor')*(col('aver_daily_dd')-5)
        mr_w_est_w =mr_w_est_w.withColumn("IBM_est", w_cons).dropDuplicates()        
        #calculate accuracy, rsquare, pearson
        mr_w_est_t = mr_w_est_w.filter((col('aver_daily_dd') >= 5) & (col('daily_consumption') > 0)).dropDuplicates()
        
        partitionWindow = Window.partitionBy(mr_w_est_t.anlage)
        naive_rms1 = functions.variance(mr_w_est_t['daily_consumption']).over(partitionWindow)
        mr_w_est_t = mr_w_est_t.withColumn("res", col('daily_consumption')-col('IBM_est')).dropDuplicates()
        model_rms1 = functions.variance(mr_w_est_t['res']).over(partitionWindow) 
        mr_w_est_t = mr_w_est_t.withColumn("R_square_score",100*(1-model_rms1/naive_rms1))
        mr_w_est_t = mr_w_est_t.withColumn("R_square_score", when((col('degree_day_factor') > 0), col("R_square_score")).otherwise(lit('0'))).dropDuplicates()
        mr_w_est_t = mr_w_est_t.withColumn("R_square_score",when((col('R_square_score') > 0.1), col('R_square_score')).otherwise(lit('0')))
        
        #Caculate Corr Coeff                                   
        corr_coef = functions.corr(col('aver_daily_dd'),col('daily_consumption')).over(partitionWindow)
        mr_w_est_t = mr_w_est_t.withColumn("corr_coef", 100*corr_coef)
        mr_w_est_t = mr_w_est_t.withColumn("corr_coef",when((col('corr_coef') > 0.1), col('corr_coef')).otherwise(lit('0')))
        mr_w_est_t = mr_w_est_t.withColumn("corr_coef", when((col('degree_day_factor') > 0), col("corr_coef")).otherwise(lit('0'))).dropDuplicates()
               
        mr_w_est_t = mr_w_est_t.withColumn("accuracy",0.25*col('R_square_score')+0.75*col('corr_coef'))

        mod_col = ['anlage','summer_baseload', 'winter_baseload', 'degree_day_factor','corr_coef','R_square_score','accuracy']
        mr_w_est = cons_df_for_model.join(mr_w_est_t[mod_col].dropDuplicates(),'anlage', how='left')
        mr_w_est = mr_w_est.withColumn("summer_baseload", when((col('summer_baseload') >= 0), col('summer_baseload')).otherwise(col('summer_cons'))).dropDuplicates()
        mr_w_est = mr_w_est.withColumn("winter_baseload", when((col('winter_baseload') >= 0), col('winter_baseload')).otherwise(col('median_winter_cons'))).dropDuplicates()
        mr_w_est = mr_w_est.withColumn("degree_day_factor", when((col('degree_day_factor') >= 0), col('degree_day_factor')).otherwise(lit('0'))).dropDuplicates()
        
        for out_col in ['corr_coef','R_square_score','accuracy']:
            mr_w_est = mr_w_est.withColumn(out_col, when((col(out_col) >= 0), col(out_col)).otherwise(lit('0'))).dropDuplicates()
        
        mr_w_est = mr_w_est.withColumn('model_created_date', lit(str(model_date.strftime('%Y%m%d'))))
        
        mod_col = ['summer_baseload', 'winter_baseload', 'degree_day_factor','corr_coef','R_square_score','accuracy']
        
        for out_col in mod_col:
            mr_w_est = mr_w_est.withColumn(out_col, col(out_col).cast('decimal(10,3)'))          
                   
        out_col = ['anlage']+mod_col
        out_col += ['model_created_date']
        
        mr_w_est = mr_w_est.where(col('winter_baseload').isNotNull())
        
        ln_model_coeffs_df = mr_w_est[out_col].dropDuplicates()
        
        spark.catalog.clearCache()      
        return ln_model_coeffs_df,  _fin_results_df

                                           
    def select_params(self, model_1, model_2, model_3):
        print("4. MODEL SELECTION")
        
        for col_name in model_1.columns:
            if col_name != 'anlage':
                model_1 = model_1.withColumnRenamed(col_name, col_name+'_10')
                model_2 = model_2.withColumnRenamed(col_name, col_name+'_4')
                model_3 = model_3.withColumnRenamed(col_name, col_name+'_2')
   
        com_model = model_1.join(model_2, ['anlage'], how='left').dropDuplicates()
        com_model = com_model.join(model_3, ['anlage'], how='left').dropDuplicates()
        com_model = com_model.withColumn("model_created_date", col("model_created_date_10"))
        
        mod_col = ['summer_baseload', 'winter_baseload', 'degree_day_factor','corr_coef','r_square_score','accuracy']
        
        cond_1 = (com_model.accuracy_10 < 60) & ((com_model.accuracy_4-com_model.accuracy_10) >= 20)        
        cond_1 = (com_model.accuracy_10 < 70) & (com_model.accuracy_4 > com_model.accuracy_10) & (com_model.accuracy_4 >= 70)  
        cond_1 = (com_model.accuracy_4 > com_model.accuracy_10)     
        
        for col_name in mod_col:
            com_model = com_model.withColumn(col_name, when((cond_1), com_model[col_name+'_4']).otherwise(com_model[col_name+'_10']))
        com_model = com_model.withColumn("Selected_Model", when((cond_1), lit('4')).otherwise(lit('10')))        
       
        cond_2 = (com_model.accuracy < 60) & ((com_model.accuracy_2-com_model.accuracy) >= 20)        
        cond_2 = (com_model.accuracy < 70) & (com_model.accuracy_2 >= 80) & (com_model.corr_coef < 100)         
        cond_2 = (com_model.accuracy < 70) & ((com_model.accuracy_2>com_model.accuracy)) & (com_model.corr_coef_2 < 100)
        
        com_model = com_model.withColumn("Selected_Model", when((cond_2), lit('2')).otherwise(col('Selected_Model')))        
        
        for col_name in mod_col:
            com_model = com_model.withColumn(col_name, when((cond_2), com_model[col_name+'_2']).otherwise(com_model[col_name]))
        
        spark.catalog.clearCache()       

        mod_col = ['summer_baseload', 'winter_baseload', 'degree_day_factor']
                    
        out_col = ['anlage']+mod_col
        out_col += ['model_created_date']
        
        ln_model = com_model[out_col].dropDuplicates()
        
        spark.catalog.clearCache()
  
        return com_model, ln_model

                                           
    def merge_model(self, read_data_df, final_model):
        
        print("5. MERGE FACTORS WITH METER READS")
        
        mr_w_est = read_data_df.join(final_model.dropDuplicates(),'anlage', how='left')
        
        s_cons = col('summer_baseload')*col('periodic_days')
        w_cons = col('winter_baseload')*col('periodic_days') + col('degree_day_factor')*(col('periodic_dd')-5*col('periodic_days'))
        mr_w_est = mr_w_est.withColumn("IBM_est_model", when((col('aver_daily_dd') <5), s_cons).otherwise(w_cons)).dropDuplicates()               
        mr_w_est = mr_w_est.withColumn("to_date", col('to_date').cast('string'))
        
        spark.catalog.clearCache() 
        return mr_w_est
        


def main():

    print("CREATING METER ESTIMATION FACTORS")
    if len(sys.argv) > 1:
        model_date = datetime.strptime(sys.argv[1],('%Y%m%d'))
    else:
        model_date = datetime.now()#.strptime('%Y%m%d')

    Model = BillingEstimator(model_date)
    #Model = BillingEstimator('20180731')
    
    mr_table = spark.sql("select * from cis_restrict_tal.meter_read")
    te107_table = spark.sql("select * from cis_restrict_tal.te107")
    mr_w_dd_df = Model.create_new_meter_table(mr_table, te107_table, Model.date)
    #mr_w_dd_df = spark.sql("select * from cis_restrict_tal.meter_read_w_dd")
    cons_df, cons_df_for_model = Model.load_mr_df(mr_w_dd_df, Model.date)
         
    four_year_past = str((Model.date-timedelta(days=365*4)).strftime('%Y%m%d'))
    two_year_past = str((Model.date-timedelta(days=365*2)).strftime('%Y%m%d'))
    four_year_past = (Model.date-timedelta(days=365*4))
    two_year_past = (Model.date-timedelta(days=365*2))
    
    #print(f"Full models are being built using consumption data from 2010-01-01 to {Model.date.strftime('%Y%m%d')}")
    #print(f"Four year models are being built using consumption data from {four_year_past} to {Model.date.strftime('%Y%m%d')}")  
    #print(f"Two year models are being built using consumption data from {two_year_past} to {Model.date.strftime('%Y%m%d')}")  
       
    count = 0
    for data in [cons_df_for_model, cons_df_for_model.filter(col('to_date') >= four_year_past), cons_df_for_model.filter(col('to_date') >= two_year_past)]:        
        model, ind_model=  Model.fit_model(data, Model.date)
           
        if count == 0:
            model10 = model
            '''
            ind_model.createOrReplaceTempView("model10_df")
            spark.sql("drop table IF EXISTS cis_restrict_tal.model10y")
            spark.sql("create table cis_restrict_tal.model10y as SELECT * from model10_df")
            '''
            spark.catalog.clearCache()
            
        elif count == 1:
            model4 = model
            '''
            ind_model.createOrReplaceTempView("model4_df")
            spark.sql("drop table IF EXISTS cis_restrict_tal.model4y")
            spark.sql("create table cis_restrict_tal.model4y as SELECT * from model4_df")
            '''
            spark.catalog.clearCache()
        else:
            model2 = model      
            '''
            ind_model.createOrReplaceTempView("model2_df")
            spark.sql("drop table IF EXISTS cis_restrict_tal.model2y")
            spark.sql("create table cis_restrict_tal.model2y as SELECT * from model2_df")
            '''
            spark.catalog.clearCache() 
        count += 1    
    
    com_models, ln_model_coeffs = Model.select_params(model10, model4, model2)
    
    spark.catalog.clearCache()
    
    com_models.createOrReplaceTempView("out_df")
    spark.sql("drop table IF EXISTS cis_restrict_tal.com_models")
    spark.sql("create table cis_restrict_tal.com_models as SELECT * from out_df")
    spark.catalog.clearCache()    
    
    ln_model_coeffs.createOrReplaceTempView("out_df")
    spark.sql("drop table IF EXISTS cis_restrict_tal.ln_model_coeffs")
    spark.sql("create table cis_restrict_tal.ln_model_coeffs as SELECT * from out_df")
    spark.catalog.clearCache()  
    
    
    #Create a merged table with IBM estimated consumptions
    merge_mod_data = Model.merge_model(cons_df, com_models)
    merge_mod_data.createOrReplaceTempView("out_df")
    spark.sql("drop table IF EXISTS cis_restrict_tal.mr_w")
    spark.sql("create table cis_restrict_tal.mr_w as SELECT * from out_df")
    

if __name__ == "__main__":
    first_start_time = datetime.now()
    print("start time", first_start_time)
    main()
    end_time = datetime.now()
    print("end time", end_time)
    print("total duration: ", end_time-first_start_time)

spark.stop()
