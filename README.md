# Real-time-battleroyale-data-streaming
Real-time-battleroyale-data-streaming

โปรเจคนี้่จะ simulate การแสดงข้อมูลการต่อสู้ของ player ต่างๆในเกมนั้นๆ โดยจะแสดงเป็นข้อมูลอาวุธ ตำแหน่งที่ทำการสังหาร pattern ในการสังหาร และมีการคำนวนคะแนนที่ได้
จาก requirements ต่างๆของอาวุธที่ใช้ และ pattern ในการสังหารควบคู่ไปด้วย และบันทึกข้อมูลลงใน bigquery เพื่อที่จะแสดงข้อมูลในรูปแบบตารางแบบชัดเจน

# ตัวอย่างของข้อมูลที่ Generate ขึ้น
ข้อมูล Generate ขึ้นจาก file Generate_message.py โดยจะ Generate ข้อมูลดังนี้
1. player_name เป็นชื่อของผู้เล่นที่ทำการสังหาร
2. weapon เป็นอาวุธที่ผู้เล่นใช้ในการสังหาร ซึ่งมี 6 ประเภทอาวุธคือ 1.Rifle 2.Pistol 3.Knife 4.Ak47 5.Shotgun 6.Sniper
3. map_location ตำแหน่งที่ทำการสังหาร
4. kill_pattern pattern ที่ใช้ในการสังหาร ซึง่มี 3 ประเภทของการสังหารนั่นคือ 1.Headshot 2.Normal 3.Lastshot
5. player_killed_name เป็นชื่อของผู้เล่นที่โดนสังหาร

# Requirements ในการคำนวนคะแนนที่ได้จากการสังหาร
