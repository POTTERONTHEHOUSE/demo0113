import socket, random, json, threading, math, time

from setting import *


batch = 0
workload = 0
completed_workload = 0

def start_task(conf):
    print("Starting Algorithm with Setting:")
    print(conf)
    global workload
    global completed_workload
    global batch
    batch = 0
    workload = conf['workload']
    completed_workload = 0

def main():
    global batch
    global workload
    global completed_workload

    s = socket.socket()
    s.connect(('localhost', PROXY_PORT))
    s.sendall("sync".encode())


    f = open("total-params.json","r")
    params = json.load(f)
    f.close()

    while True: # pulling start msg
        env_setting = s.recv(4096).decode()
        conf = json.loads(env_setting)
        galaxy_proxy = threading.Thread(target=start_task, args=(conf,))
        galaxy_proxy.start()
        phase = "training"

        tmp_params = params[conf['system']][conf['dataset']][conf['algorithm']][conf['num_of_machines']]

        while True:
            s.sendall("pulling query msg".encode())
            s.recv(1024).decode()


            # Random process
            last_workload = completed_workload

            if random.random() < 0.2:
                tmp_workload = int(math.ceil(0.5*workload))
            else:
                tmp_workload = random.randrange(0,int(math.ceil(0.5*workload))+1)

            batch += 1
            tmp_workload = batch*32
            maxHWM = tmp_params["HWM"][0]*math.pow(tmp_workload, tmp_params["HWM"][1]) + tmp_params["HWM"][2]
            maxRSS = tmp_params["RSS"][0]*math.pow(tmp_workload, tmp_params["RSS"][1]) + tmp_params["RSS"][2]
            maxHWM = 4*1024*(random.random() - 0.5) + maxHWM
            maxRSS = 1024*(random.random() - 0.5) + maxRSS
            completed_workload += tmp_workload
            if completed_workload > 0.5*workload and phase == "training":
                phase = "evaluation"
            if completed_workload > workload:
                tmp_workload = workload - last_workload
                phase = "end"
            time_cost = 20*random.random() + 10
            time.sleep(time_cost+batch*5)
            result = [{"phase":phase,"batch":batch,"workload":tmp_workload,"maxHWM":maxHWM,"maxRSS":maxRSS,"metric":random.random(),"cost_time":time_cost}]
            s.sendall(json.dumps(result).encode())

            if phase == "end":
                break



        s.sendall("pulling start msg".encode())
        phase = "training"
        batch = 0
        workload = 0
        completed_workload = 0



if __name__ == '__main__':
    main()
