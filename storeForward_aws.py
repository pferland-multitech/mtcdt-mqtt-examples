#!/usr/bin/python

import time
import io, json 					#needed for exporting payloads to json file
import paho.mqtt.client as mqtt

import argparse
import logging
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient


'''
	@authors Madeline Prins, Peter Ferland, Jason Reiss and Jeff Hatch

	For help, see paho documentation: https://www.eclipse.org/paho/clients/python/docs/

	This demo script prints out packets when there is a solid ethernet connection
	and sends them to a JSON file when the connection is down. On a reconnection,
	the JSON file is checked for packets, printing out the stored packets, and emptying the file.

	##### INSTRUCTIONS #####
	1.Make serial connection with conduit in tera term/minicom
	2.execute: cd /opt/serial-streamer
	3.Add storeForward.py into this directory on the conduit
	4.Create new empty json file with name you give to the jsonFilePath variable below
	(you only have to do this once)
	4.Run script with command: ./storeForward.py
	**NOTE: You make need to give permission to run this script. Use the command:
	chmod +x storeForward.py

	**NOTE: When testing, note that it may take several minutes for disconnections and reconnections
	to be realized.

	Adapted for AWS based on https://raw.githubusercontent.com/aws/aws-iot-device-sdk-python/master/samples/basicPubSub/basicPubSub.py
	
 '''

class mqttStoreForward:

	#class variables
	isConnected = False 					#checks ethernet connection
	lora_client = mqtt.Client()
	packet = None 							#will carry msg payload...empty to begin
	isJsonEmpty = True 						#keep track of whether file is empty
	jsonFilePath = 'packetStorage.json'		#INPUT DESIRED JSON FILE NAME HERE OR LEAVE DEFAULT
	#serverTopic = "practice/topic"			#INPUT DESIRED TOPIC TO SUB/PUB TO HERE OR LEAVE DEFAULT
	
	#aws related variables
	host = ""
	rootCAPath = ""
	certificatePath = ""
	privateKeyPath = ""
	port = ""
	useWebsocket = ""
	clientId = ""
	topic = ""
	myAWSIoTMQTTClient = None

	def __init__ (self):
		#touch jston file if it does not exist
		file = open(self.jsonFilePath, 'a')
		file.close()
		
	def parseArgs(self):
		parser = argparse.ArgumentParser()
		parser.add_argument("-e", "--endpoint", action="store", required=True, dest="host", help="Your AWS IoT custom endpoint")
		parser.add_argument("-r", "--rootCA", action="store", required=True, dest="rootCAPath", help="Root CA file path")
		parser.add_argument("-c", "--cert", action="store", dest="certificatePath", help="Certificate file path")
		parser.add_argument("-k", "--key", action="store", dest="privateKeyPath", help="Private key file path")
		parser.add_argument("-p", "--port", action="store", dest="port", type=int, help="Port number override")
		parser.add_argument("-w", "--websocket", action="store_true", dest="useWebsocket", default=False,help="Use MQTT over WebSocket")
		parser.add_argument("-id", "--clientId", action="store", dest="clientId", default="basicPubSub",help="Targeted client id")
		parser.add_argument("-t", "--topic", action="store", dest="topic", default="sdk/test/Python", help="Targeted topic")
		args = parser.parse_args()
		self.host = args.host
		self.rootCAPath = args.rootCAPath
		self.certificatePath = args.certificatePath
		self.privateKeyPath = args.privateKeyPath
		self.port = args.port
		self.useWebsocket = args.useWebsocket
		self.clientId = args.clientId
		self.topic = args.topic

		if args.useWebsocket and args.certificatePath and args.privateKeyPath:
			parser.error("X.509 cert authentication and WebSocket are mutual exclusive. Please pick one.")
			exit(2)

		if not args.useWebsocket and (not args.certificatePath or not args.privateKeyPath):
			parser.error("Missing credentials for authentication.")
			exit(2)

		# Port defaults
		if args.useWebsocket and not args.port:  # When no port override for WebSocket, default to 443
			self.port = 443
		if not args.useWebsocket and not args.port:  # When no port override for non-WebSocket, default to 8883
			self.port = 8883
		
	def configAWSLogging(self):
		# Configure logging
		self.logger = logging.getLogger("AWSIoTPythonSDK.core")
		self.logger.setLevel(logging.DEBUG)
		streamHandler = logging.StreamHandler()
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		streamHandler.setFormatter(formatter)
		self.logger.addHandler(streamHandler)
		
	#connect lora client to localhost
	def setLoraClient(self):
		self.lora_client.connect("127.0.0.1")

	#connect remote client to PC IP address
	def setRemoteClient(self):
		# Init AWSIoTMQTTClient
		if self.useWebsocket:
			self.myAWSIoTMQTTClient = AWSIoTMQTTClient(self.clientId, useWebsocket=True)
			self.myAWSIoTMQTTClient.configureEndpoint(self.host, self.port)
			self.myAWSIoTMQTTClient.configureCredentials(self.rootCAPath)
		else:
			self.myAWSIoTMQTTClient = AWSIoTMQTTClient(self.clientId)
			self.myAWSIoTMQTTClient.configureEndpoint(self.host, self.port)
			self.myAWSIoTMQTTClient.configureCredentials(self.rootCAPath, self.privateKeyPath, self.certificatePath)
		# AWSIoTMQTTClient connection configuration
		self.myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
		self.myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
		self.myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
		self.myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
		self.myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec
		
		self.myAWSIoTMQTTClient.onOnline = self.remoteOnConnect
		self.myAWSIoTMQTTClient.onOffline = self.onRemoteDisconnect

		# Connect and subscribe to AWS IoT
		self.myAWSIoTMQTTClient.connect()
#		self.myAWSIoTMQTTClient.subscribe(self.topic, 1, )

	#callback function initiated on on_connect property for lora client
	def loraOnConnect(self, client, userdata, flags, rc):
		print("Lora Client Connection: " + str(rc)) 	#Returns a 0
		self.lora_client.subscribe("lora/+/up", qos=0)

	#callback function initiated on on_connect property for remote client
	def remoteOnConnect(self):
		self.isConnected = True
		print("Remote Client Connection: " + str(self.isConnected))

	#callback function initiated on on_disconnect property for both clients
	def onDisconnect(self, client, userdata, rc):
		self.isConnected = False
		print("The connection has failed.")

	def onRemoteDisconnect(self):
		self.isConnected = False
		print("AWS Connection failed")

	#call back function initiated on on_message
	def onMessage(self, mqtt_client, userdata, msg):
		self.packet = msg.payload
		self.myAWSIoTMQTTClient.publish(self.topic, self.packet, 1)
		self.checkConnect(self.packet) #handle packet

	#set callback properties of both clients to their respective functions(from above)
	def setVals(self):
		self.lora_client.on_connect = self.loraOnConnect
		self.lora_client.on_message = self.onMessage
		self.lora_client.on_disconnect = self.onDisconnect

	#takes packet parameter and appends it to a file
	def writeToJson(self, data):
		with open(self.jsonFilePath, 'a') as myFile:
			myFile.write(data + "\r\n")

	#Controls what is done with the packet depending on a working/not working connection
	def checkConnect(self, packet):
		if(self.isConnected == True):
			#check whether the file is empty/has stored packets every time the
			#connection is found to be good
			self.checkJsonFile()
			##################################################################################
			#ADD YOUR CODE HERE. WHEN THE CONNECTION IS WOKRING, DECIDE WHAT TO DO WITH PACKET
			##################################################################################
			print("PRINTING PACKET/CONNECTION OK")
			print("PRINTED: " + packet)

		else:
			#When the connection is bad, we write the packet to the json file for storage.
			print("ADDING TO JSON FILE. CONNECTION DOWN") #Ethernet down
			'''FOR TESTING, make sure that this packet matches one printed when it reconnects
			 and forwards packets from json storage: '''
			print("STORED: " + packet)
			self.writeToJson(packet) #store


	#Creates infinite loop needed for paho mqtt loop_forever()
	def runLoop(self):
		while(True):
			time.sleep(1)

	#Creates event loop and new thread that initializes the paho mqtt loops for both clients
	def startLoop(self):
		#UI thread = terminal interaction
		self.lora_client.loop_start()
		

	'''Check the json file to see if it's empty. If it is, do nothing, if there are stored
	packets, forward them, line by line'''
	def checkJsonFile(self):
		with open(self.jsonFilePath, 'r') as output:
			output.seek(0) #go to beginning of file
			charTrue = output.read(1) #retrieve first character of json file, if it exists
			if not charTrue:
				print("File is empty. Nothing to forward.")
			else:
				#file is not empty and we have data to forward
				print("FORWARDING DATA!!!!")
				with open(self.jsonFilePath) as forwardFile:
					linesGroup = forwardFile.read().splitlines()
					forwardFile.close()
					#############################################################################
					'''ADD YOUR CODE HERE! WHEN THERE ARE PACKETS STORED IN THE JSON FILE, DECIDE
					 WHAT TO DO WITH THEM WHEN THE CONNECTION IS BACK UP AGAIN'''
					#############################################################################
					for line in linesGroup:
						print("FORWARDING STORED PACKET")
						#publish the data to the server topic!!!
						self.remote_client.publish(self.serverTopic, line)
						print(line)

				#After all data is forwarded, CLEAR THE FILE!!
				with open(self.jsonFilePath, 'r+') as clearFile:
					clearFile.truncate(0)

def main():
	instance = mqttStoreForward() 	#create instance of class
	instance.configAWSLogging()
	instance.parseArgs()
	instance.startLoop()
	#need to call setVals first because they wont connect to the call backs if the setClient functions are called first
	instance.setVals() 				#set connect and message properties & infinite loop
	instance.setLoraClient() 		#connect to local host
	instance.setRemoteClient() 		#connect to PC IP address
	instance.runLoop()

if __name__ == "__main__":
	main()
