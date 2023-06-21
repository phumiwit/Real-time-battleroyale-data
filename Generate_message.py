import datetime, json, os, random, time
project = 'your project id'
# playname_สำหรับสุ่ม
player_name = ['John','Kim','Gift','Lee','Andrew','Allison','Arthur','Ivan','Biscuit','Oliva','Lay','Justin','Taylor','Sam','Mint','Mark','Lala','Omar','Chris','Alan']
# อาวุธสำหรับสุ่ม
weapon = ['Rifle','Pistol','Knife','Ak47','Shotgun','Sniper']
# ตำแหน่งที่ทำการสังหารสำหรับสุ่ม
map_location = ['MP_114','MP_104','MP_103','MP_111','MP_110','MP_115','MP_108','MP_113','MP_107','MP_109','MP_105','MP_100','MP_101','MP_102','MP_112','MP_106']
# kill_pattern สำหรับสุ่ม
kill_pattern = ['Headshot','Normal','Lastshot']

# ทำการ Generate data
while True:
    # Generate player_name โดยที่ไม่ให้ player ที่สังหารซ้ำกับ player ที่โดนสังหาร (ไม่มีการสังหารตนเอง)
    player_name1 = random.choice(player_name)
    player_name2 = random.choice(player_name)
    while player_name1 == player_name2:
            player_name2 = random.choice(player_name)

    data = {
            'player_name':player_name1,
            'weapon':random.choice(weapon),
            'map_location':random.choice(map_location),
            'kill_pattern':random.choice(kill_pattern),
            'player_killed_name':player_name2,
            
    }
    message = json.dumps(data)
    command = "gcloud --project={} pubsub topics publish game-score-killpattern-data --message='{}'".format(project, message)
    print(command)
    os.system(command)
    time.sleep(random.randrange(1, 2))