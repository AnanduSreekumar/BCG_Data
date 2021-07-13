#Initiatl version created by Anandu Sreekumar - 12th July, 2021
#created for BCG 
# Read pdf for instruction
import pyspark
import os.path
import json
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession,functions as F
conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
sc = pyspark.SparkContext(conf=conf)
spark = SparkSession(sc)
config_path = 'C:/Users/ANANDU/Desktop/BCG/Config/config.json'
df_list = {}
global df_join_person_units,df_join_person_units_damages,df_join_person_units_damages_charges,log_body,prog_status
prog_status = "SUCCESS"
#saves log in a string obj

def save_log(log):
    global log_body
    print(str(log))
    log_body += str(log)+"\n"
#read config data
def read_config():
    global prog_status
    config = {}
    try:
        f = open(config_path,)
    except :
        prog_status = "FAILURE"
        print("Config failed to open!!")
    else:
        print("Successfully retrieved log!!")
        config = json.load(f)
        f.close()
    return config
if __name__ == "__main__":
    log_body = " "
    #getting config info 
    config = read_config()
    print(str(config))
    file_path = config["file_path"]
    output_path = config["output_path"]
    raw_file_name = config["raw_file_name"].split(',')
    print("File path : "+file_path)
    print("output_path : "+output_path)
    print("raw_file_name : "+str(raw_file_name))
    save_log("Analysis Report BCG - Candidate Anandu")
def getData(file_loc):
    global prog_status
    try:
        df = spark.read.options(delimiter=',',header='True',inferSchema='True').csv(file_loc)
        save_log("File count: "+str(df.count()))
    except:
        prog_status = "FAILURE"
        print("Failed to read file!")
    return df.distinct()
def initiate_file_scan(file_names,file_path_dir,file_type):
    #based on the file name provided in config file , this will initiate data read
    for i in file_names:
        save_log("File : "+i)
        df_list["df_"+i.lower()] = getData(file_path_dir+i+file_type)
def joinTables():
    global df_join_person_units,df_join_person_units_damages,df_join_person_units_damages_charges
    df_join_person_units = df_list['df_units_use'].join(df_list['df_primary_person_use'],["CRASH_ID","UNIT_NBR"]  ,"full")
    df_join_person_units_damages = df_join_person_units.dropDuplicates(['CRASH_ID', 'UNIT_NBR']).join(df_list['df_damages_use'],["CRASH_ID"] , 'left')
    df_join_person_units_damages_charges = df_join_person_units_damages.dropDuplicates(['CRASH_ID', 'UNIT_NBR','PRSN_NBR']).join(df_list['df_charges_use'],['CRASH_ID', 'UNIT_NBR','PRSN_NBR'] , 'left')
    df_join_person_units_damages_charges = df_join_person_units_damages_charges.dropDuplicates(['CRASH_ID', 'UNIT_NBR','PRSN_NBR'])
def analysis1():
    try:
        save_log("\nAnalysis 1:")
        save_log("Q:Find the number of crashes (accidents) in which number of persons killed are male? ")
        save_log("Answer:")
        count = df_list['df_primary_person_use'].dropDuplicates(['CRASH_ID','PRSN_NBR','UNIT_NBR']).filter((df_list['df_primary_person_use'].PRSN_GNDR_ID == "MALE") & (df_list['df_primary_person_use'].DEATH_CNT!=0)).count()    
    except Exception as error:
        save_log("Analysis Failed! Error; "+str(error))
    else:
        save_log("number of male killed in crashes: "+str(count))
    return
def analysis2():
    try:
        save_log("\nAnalysis 2:")
        save_log("Q:How many two wheelers are booked for crashes? ")
        count1 = df_join_person_units_damages_charges.filter(df_join_person_units_damages_charges.PRSN_TYPE_ID.isin(['DRIVER OF MOTORCYCLE TYPE VEHICLE','PEDALCYCLIST'])  ).count()
        count2 = df_join_person_units_damages_charges.dropDuplicates(['VIN']).filter(df_join_person_units_damages_charges.PRSN_TYPE_ID.isin(['DRIVER OF MOTORCYCLE TYPE VEHICLE','PEDALCYCLIST'])  ).count()
    except Exception as error:
        save_log("Analysis Failed! Error; "+str(error))
    else:
        save_log("Total 2 wheeler crashes count:"+str(count1))
        save_log("Distinct 2 wheeler crashes count (based on VIN):"+str(count2))
    return
def analysis3():
    try:
        save_log("\nAnalysis 3:")
        save_log("Q:Which state has highest number of accidents in which females are involved?  ")
        save_log("Answer:")
        df_analysis3 = df_join_person_units_damages_charges.filter((df_join_person_units_damages_charges.PRSN_GNDR_ID == 'FEMALE')).groupBy('DRVR_LIC_STATE_ID').count()
        output_3 = df_analysis3.orderBy(df_analysis3['count'].desc()).collect()[0][0]
    except Exception as error:
        save_log("Analysis Failed! Error; "+str(error))
    else:
        save_log("State : "+str(output_3))
    return
def analysis4():
    try:
        save_log("\nAnalysis 4:")
        save_log("Q:Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death ")
        save_log("Answer:")
        df_analysis4 = df_list['df_units_use'].dropDuplicates(['CRASH_ID','UNIT_NBR']).filter(df_list['df_units_use'].VEH_MAKE_ID!="NA").groupBy('VEH_MAKE_ID').sum('TOT_INJRY_CNT','DEATH_CNT')
        df_analysis4 = df_analysis4.withColumn("total", df_analysis4['sum(TOT_INJRY_CNT)']+df_analysis4['sum(DEATH_CNT)'])
        df_analysis4 = df_analysis4.orderBy(df_analysis4['total'].desc()).head(20)
        analysis4_list = [row.VEH_MAKE_ID for row in df_analysis4]
        output_4 = " Top 5th to 15th VEH_MAKE_IDs : "
        for i in range(4,15):
            output_4 += str(analysis4_list[i])+","
        print(output_4)
    except Exception as error:
        save_log("Analysis Failed! Error; "+str(error))
    else:
        save_log(output_4)
    return
def analysis5():
    try:
        save_log("\nAnalysis 5:")  
        save_log("Q:For all the body styles involved in crashes, mention the top ethnic user group of each unique body style  ")
        save_log("Answer:")
        veh_bod_style_row = df_join_person_units_damages_charges.select("VEH_BODY_STYL_ID").distinct().filter(df_join_person_units_damages_charges.VEH_BODY_STYL_ID.isNotNull())
        veh_bod_style_list = [row.VEH_BODY_STYL_ID for row in veh_bod_style_row.collect()]
        for style in veh_bod_style_list:
            df_analysis5 = df_join_person_units_damages_charges.filter(df_join_person_units_damages_charges.VEH_BODY_STYL_ID == style).groupBy('PRSN_ETHNICITY_ID').count()
            ethinicity = df_analysis5.orderBy(df_analysis5['count'].desc()).collect()[0][0]
            save_log("For body "+str(style)+" style max occurance in "+str(ethinicity)+" ethinicity.")
    except Exception as error:
        save_log("Analysis Failed! Error; "+str(error))
    return
def analysis6():
    try:
        save_log("\nAnalysis 6:")
        save_log("Q:Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code) ")
        save_log("Answer:")
        df_analysis6 = df_join_person_units_damages_charges.filter((df_join_person_units_damages_charges.PRSN_ALC_RSLT_ID == 'Positive') & (df_join_person_units_damages_charges.UNIT_DESC_ID == 'MOTOR VEHICLE') & (df_join_person_units_damages_charges.DRVR_ZIP!='null') ).groupBy('DRVR_ZIP').count()
    except Exception as error:
        save_log("Analysis Failed! Error; "+str(error))
    else:
        save_log("Top 5 Zip Codes : "+str([row.DRVR_ZIP for row in df_analysis6.orderBy(df_analysis6['count'].desc()).head(5)]) )
    return
def analysis7():
    try:
        save_log("\nAnalysis 7:")
        save_log("Q:Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance")
        save_log("Answer:")
        count =df_join_person_units_damages.dropDuplicates(['CRASH_ID']).filter((df_join_person_units_damages.UNIT_DESC_ID == 'MOTOR VEHICLE') & ((df_join_person_units_damages.FIN_RESP_TYPE_ID != 'NA') | (df_join_person_units_damages.FIN_RESP_TYPE_ID.isNotNull())) & ((df_join_person_units_damages.DAMAGED_PROPERTY.isNull()) | (df_join_person_units_damages.DAMAGED_PROPERTY == 'NONE')) & ((df_join_person_units_damages.VEH_DMAG_SCL_1_ID.rlike('[5-9]')) | (df_join_person_units_damages.VEH_DMAG_SCL_2_ID.rlike('[5-9]')))).count()
    except Exception as error:
        save_log("Analysis Failed! Error; "+str(error))
    else:
        save_log("count "+str(count))
    return
def analysis8():
    try:
        save_log("\nAnalysis 8:")
        save_log("Q:Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)")
        save_log("Answer:")
        #get top 25 states with major offence
        states_offence = df_join_person_units_damages_charges.filter(df_join_person_units_damages_charges.CHARGE.isNotNull()).groupBy("VEH_LIC_STATE_ID").count()
        top_25_states_row = states_offence.select('VEH_LIC_STATE_ID').orderBy(states_offence['count'].desc()).head(25)
        top_25_states_list = [row.VEH_LIC_STATE_ID for row in top_25_states_row]
        #get top 10 color 
        df_vehicle_color_count = df_join_person_units_damages_charges.dropDuplicates(['VIN']).groupBy('VEH_COLOR_ID').count()
        df_top_10_vehicle_color_row = df_vehicle_color_count.select('VEH_COLOR_ID').orderBy(df_vehicle_color_count['count'].desc()).head(10)
        df_top_10_vehicle_color_list = [row.VEH_COLOR_ID for row in df_top_10_vehicle_color_row]
        df_analysis8 = df_join_person_units_damages_charges.filter((df_join_person_units_damages_charges.CHARGE.contains('SPEED')) & (df_join_person_units_damages_charges.DRVR_LIC_CLS_ID != 'UNLICENSED') & (df_join_person_units_damages_charges.VEH_COLOR_ID.isin(df_top_10_vehicle_color_list)) & (df_join_person_units_damages_charges.VEH_LIC_STATE_ID.isin(top_25_states_list))).groupBy('VEH_MAKE_ID' ).count()
    except Exception as error:
        save_log("Analysis Failed! Error; "+str(error))
    else:
        save_log("Top 5 Vehicle Makes : "+str([row.VEH_MAKE_ID for row in df_analysis8.orderBy(df_analysis8['count'].desc()).head(5)]) )
    return
def save_report():
    path =  os.path.join(output_path, "report.txt")
    try: 
        os.mkdir(path) 
    except OSError as error: 
        save_log("Warning! File creation failure : "+str(error))
    outFile=open(path, "w")
    outFile.write(log_body)
    outFile.close()

if prog_status == "SUCCESS":
    initiate_file_scan(raw_file_name,file_path,".csv")
    joinTables()
if prog_status == "SUCCESS":
    analysis1()
    analysis2()
    analysis3()
    analysis4()
    analysis5()
    analysis6()
    analysis7()
    analysis8()
else:
    exit(-1)
save_report()
sc.stop()
