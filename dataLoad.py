# import libraries
import json
from db_config import get_db_info
from flatten_json import flatten
import os
import psycopg2


path = "./data"
dir_list = os.listdir(path)

# List input files
def list_input_file():
    files = []
    for lst in dir_list:
        if lst.startswith("MATCH_"):
            # files present in current folder to the list
            files.append(lst)
    return files

def dbase_Conn():
    filename='db_info.ini'
    section='postgres-db'
    db_info = get_db_info(filename,section)
    db_conn = None
    
    try:
        db_conn = psycopg2.connect(**db_info)
        print("Successfully connected to the database.")
    except Exception as e:
        print("An exception has occurred! - No connection to postgreSQL DB"+ e)

    # finally:
    #     if db_conn:
    #         db_conn.close()
    #         print("Closed connection.")

    return db_conn

# Read input files
def main():
    for file in list_input_file():
        print(file)
        file = path+'/'+str(file)
        match_id = str(file).split('_')[1]
        print(match_id)
        with open(file, 'r') as osFile:
            crkt_match  = osFile.readlines()
            dbconn = dbase_Conn()   
            db_cursor = dbconn.cursor()
            print(dbconn)
            for ball in crkt_match:

                # Parse JSON data into a dictionary
                parsed_data = json.loads(ball)
        
                # Flatten JSON data
                flat_data = flatten(parsed_data)
                #print(flat_data)

                # cricket match balls 
                mat_ins_rec = "INSERT INTO cricket.matches(match_id, is_out, runs, batting_team_id, \
                                bowling_team_id,  batter_id, non_facer_id,   bowler_id) \
                                VALUES ({},{},{},{},{},{},{},{}) \
                                ON CONFLICT (match_id,batting_team_id,bowling_team_id,batter_id,non_facer_id,bowler_id) DO NOTHING;"" \
                            ".format(match_id, str(flat_data['is_out']), str(flat_data['runs']), str(flat_data['batting_team_team_id']), \
                                        str(flat_data['bowling_team_team_id']), str(flat_data['batter_player_id']), \
                                        str(flat_data['non_facer_player_id']), str(flat_data['bowler_player_id']))
                # teams
                team_ins1 = "INSERT INTO cricket.teams(team_id,name) \
                            VALUES ({},{}) ON CONFLICT (team_id) DO NOTHING;".format(str(flat_data['batting_team_team_id']), "'"+str(flat_data['batting_team_name'])+"'")
                team_ins2 = "INSERT INTO cricket.teams(team_id,name) \
                            VALUES ({},{}) ON CONFLICT (team_id) DO NOTHING;".format(str(flat_data['bowling_team_team_id']), "'"+str(flat_data['bowling_team_name'])+"'")
                
                # Players - batter
                batter_ins = "INSERT INTO cricket.players(player_id,hand,name) \
                              VALUES ({},{},{}) ON CONFLICT (player_id) DO NOTHING;".format(str(flat_data['batter_player_id']), \
                              "'"+str(flat_data['batter_hand']) +"'","'"+str(flat_data['batter_name'])+"'")

                # Players - non_facer
                non_facer_ins = "INSERT INTO cricket.players(player_id,hand,name) \
                                 VALUES ({},{},{}) ON CONFLICT (player_id) DO NOTHING;".format(str(flat_data['non_facer_player_id']), \
                                 "'"+str(flat_data['non_facer_hand'])+"'","'"+str(flat_data['non_facer_name'])+"'")          
                
                # Players - bowler
                bowler_team_ins = "INSERT INTO cricket.players(player_id,hand,name) \
                                  VALUES ({},{},{}) ON CONFLICT (player_id) DO NOTHING;".format(str(flat_data['bowler_player_id']), \
                                  "'"+str(flat_data['bowler_hand'])+"'", "'"+str(flat_data['bowler_name'])+"'")   

                db_cursor.execute(mat_ins_rec)
                db_cursor.execute(team_ins1)  
                db_cursor.execute(team_ins2) 
                db_cursor.execute(batter_ins)  
                db_cursor.execute(non_facer_ins)  
                db_cursor.execute(bowler_team_ins)  

                dbconn.commit()

    dbconn.close()            


if __name__ == "__main__":
    main()