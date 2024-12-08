import itertools
import pandas as pd
import json
import pyspark.sql.functions as F

def create_invitations_table(spark):
  """
  Creates the delta table for invitations
  From,To,Sent At,Message,Direction,inviterProfileUrl,inviteeProfileUrl
  """
  
  invitations = spark.read.option("header", "true").csv("s3a://raw/linkedin/Invitations.csv")
  invitations = spark.read.option("header", "true").csv("s3a://raw/linkedin/Invitations.csv")
  invitations = invitations.withColumnRenamed("From", "sender")\
                          .withColumnRenamed("To", "recipient")\
                          .withColumnRenamed("Sent At", "sent_at")\
                          .withColumnRenamed("Message", "message")\
                          .withColumnRenamed("Direction", "direction")\
                          .withColumnRenamed("inviterProfileUrl", "sender_profile_url")\
                          .withColumnRenamed("inviteeProfileUrl", "recipient_profile_url")
  
  invitations.write.format("delta").mode("overwrite").save("s3a://processed/linkedin/invitations")

def create_messages_table(spark):
  """
  Creates the delta table for messages
  """
  messages = spark.read.option("header", "true").csv("s3a://raw/linkedin/messages.csv")
  messages = messages.drop('CONVERSATION ID')

  messages = messages.withColumnRenamed("CONVERSATION TITLE", "conversation_title")\
    .withColumnRenamed("FROM", "sender")\
    .withColumnRenamed("SENDER PROFILE URL", "sender_profile_url")\
    .withColumnRenamed("RECIPIENT PROFILE URLS", "recipient_profile_url")\
    .withColumnRenamed("SUBJECT", "subject")\
    .withColumnRenamed("CONTENT", "content")\
    .withColumnRenamed("FOLDER", "folder")\
    .withColumnRenamed("TO", "recipient")\
    .withColumnRenamed("DATE", "date")


  messages.write.format("delta").mode("overwrite").save("s3a://processed/linkedin/messages")
  
def create_connections_table(spark):
  """
    Creates the delta table for connections 
  """
  
  df = spark.read \
    .option("header", "true") \
    .csv('s3a://raw/linkedin/Connections.csv')
 
  df = df.withColumnRenamed("First Name", "first_name")\
    .withColumnRenamed("Last Name", "last_name")\
    .withColumnRenamed("Email Address", "email")\
    .withColumnRenamed("Company", "company")\
    .withColumnRenamed("Position", "position")\
    .withColumnRenamed("Connected On", "connected_on")\
    .withColumnRenamed("URL", "profile_url")
  
  df = df.withColumn(
    "full_name",
    F.concat("first_name", F.lit(" "), "last_name")
  )

  df.write.format("delta").mode("overwrite").save("s3a://processed/linkedin/connections")
