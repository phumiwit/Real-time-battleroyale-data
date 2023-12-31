import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
import os
from apache_beam import window
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AccumulationMode, AfterCount, Repeatedly
from datetime import datetime
import argparse
import json
import logging
import time
from typing import Any, Dict, List
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

def custom_timestamp(elements):
  unix_timestamp = elements[16].rstrip().lstrip()
  return beam.window.TimestampedValue(elements, int(unix_timestamp))

# encode byte string และ return ค่า ต่างๆ
def encode_byte_string(element):
  new_list = []
  name_list = element.split(':')
  for element in name_list:
    new_list.append(element.encode('utf-8'))
  player_name = new_list[0]
  weapon = new_list[1]
  kill_pattern = new_list[2]
  map_location = new_list[3]
  player_killed = new_list[4]
  score = new_list[5]
  return player_name,weapon,kill_pattern,map_location,player_killed,score


# คำนวนคะแนนจากการต่อสู้ โดยคะแนนขึ้นอยู่กับอาวุธและ pattern ในการฆ่า
def calculate_battle_points(element_list):     
    total_points = 0
    player_name = element_list[0]
    weapon = element_list[1]   
    map_location = element_list[2]
    kill_pattern = element_list[3]
    player_killed = element_list[4]
    
    if weapon == 'Rifle':
        if kill_pattern == 'Normal':
            total_points += 2
        elif kill_pattern == 'Lastshot':
            total_points += 1
        elif kill_pattern == 'Headshot':
            total_points += 5
    elif weapon == 'Pistol':
        if kill_pattern == 'Normal':
            total_points += 3
        elif kill_pattern == 'Lastshot':
            total_points += 1
        elif kill_pattern == 'Headshot':
            total_points += 5
    elif weapon == 'Knife':
        if kill_pattern == 'Normal':
            total_points += 3
        elif kill_pattern == 'Lastshot':
            total_points += 1
        elif kill_pattern == 'Headshot':
            total_points += 5
    elif weapon == 'Ak47':
        if kill_pattern == 'Normal':
            total_points += 2
        elif kill_pattern == 'Lastshot':
            total_points += 1
        elif kill_pattern == 'Headshot':
            total_points += 5
    elif weapon == 'Shotgun':
        if kill_pattern == 'Normal':
            total_points += 2
        elif kill_pattern == 'Lastshot':
            total_points += 1
        elif kill_pattern == 'Headshot':
            total_points += 5
    elif weapon == 'Sniper':
        if kill_pattern == 'Normal':
            total_points += 4
        elif kill_pattern == 'Lastshot':
            total_points += 1
        elif kill_pattern == 'Headshot':
            total_points += 7

       
    return player_name + ':' + weapon + ':' + kill_pattern + ':' + map_location + ':' + player_killed,total_points                 


# ทำการสกัดค่าต่างๆจาก json format
def convert_json(element_list):
    json_data = json.loads(element_list)
    element_list = list(json_data.values())
    return element_list

class PointFn(beam.CombineFn):
  def create_accumulator(self):
    return (0.0, 0)

  def add_input(self, sum_count, input):                        
    (sum, count) = sum_count                                     
    return sum + input, count + 1                                

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)                            
    return sum(sums), sum(counts)                                

  def extract_output(self, sum_count):
    (sum, count) = sum_count                                   
    return sum / count if count else float('NaN')                                  

# ดึง element ต่างๆ ของ element list และสร้างเป็น dictionary
def write_to_bigquery(element):
    player_name, weapon,kill_pattern,map_location,player_killed,score = element
    row = {
        'player_name': player_name,
        'weapon': weapon,
        'kill_pattern': kill_pattern,
        'map_location':map_location,
        'player_killed':player_killed,
        'score': score
    }
    return row

# ทำการแยกค่าที่เป็น key และ value โดนสร้าง list of key โดย split ด้วย : และสกัดค่าต่างๆออกมา
def format_result(key_value_pair):
    name, points = key_value_pair
    name_list = name.split(':')
    player_name = name_list[0]
    weapon = name_list[1]
    kill_pattern = name_list[2]
    map_location = name_list[3]
    player_killed = name_list[4]
    return  player_name + ':' + weapon + ':' + kill_pattern + ':' + map_location + ':' + player_killed + ':' + str(points) + '  points'
    
# ฟังก์ชันสำหรับรับ input 
def run(
    input_subscription: str,
    output_table: str,
    window_interval_sec: int = 60,
    beam_args: List[str] = None,
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)
    with beam.Pipeline(options=options) as pipeline:
        pubsub_data = (
            pipeline
            | 'Read from pub sub' >> beam.io.ReadFromPubSub(subscription= input_subscription)
            | 'Parse data' >> beam.Map(convert_json) # element_list
            | 'Calculate battle points' >> beam.Map(calculate_battle_points)  #player_name:weapon:kill_pattern:map_location:player_killed,total_points         
            | 'Window for player' >> beam.WindowInto(window.Sessions(30))
            | 'Group by key' >> beam.CombinePerKey(PointFn())                    
            | 'Format results' >> beam.Map(format_result)    # player_name:weapon:kill_pattern:map_location:player_killed:str(points) points
            | 'Encode data to byte string' >> beam.Map(encode_byte_string) # player_name,weapon,kill_pattern,map_location,player_killed,score
            | 'Prepare data for BigQuery' >> beam.Map(write_to_bigquery)   # row
            # บันทึกข้อมูลลง Bigquery
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                output_table,
                schema='player_name:STRING, weapon:STRING,kill_pattern:STRING,map_location:STRING,player_killed:STRING,score:STRING',
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                )
            )
                

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
    )
    parser.add_argument(
        "--input_subscription",
        help="Input PubSub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        "--window_interval_sec",
        default=60,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )
    args, beam_args = parser.parse_known_args()

    run(
        input_subscription=args.input_subscription,
        output_table=args.output_table,
        window_interval_sec=args.window_interval_sec,
        beam_args=beam_args,
    )