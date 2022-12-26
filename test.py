import os
import random
import shutil
 
# Get the list of all files and directories
path = "/Users/rahul/Downloads/archive/raw-img/cane/"
dir_list = os.listdir(path)

print("Files and directories in '", path, "' :")
 
# prints all files
# print(dir_list)

sample_images = random.sample(dir_list, 200)

dst_dir = "/Users/rahul/Dev/college/CS425_Distributed_Systems/MPS/MP4/awesomedml/testfiles/"


i = 1
for file in sample_images:
    shutil.copy(path + file, dst_dir + f"{i}.jpeg")
    i += 1

"""
# inceptionV3_running_jobs = self.get_count_of_running_nodes('InceptionV3')
            # resnet50_running_jobs = self.get_count_of_running_nodes('ResNet50')
            # free_workers = list(set(self.worker_nodes) - set(list(self.workers_tasks_dict.keys())))

            # # inceptionV3_running_jobs += len(free_workers)
            # # bigger_model_vmcount = max(inceptionV3_running_jobs, resnet50_running_jobs)
            # # smaller_model_vmcount = min(inceptionV3_running_jobs, resnet50_running_jobs)

            # if inceptionV3_running_jobs >= resnet50_running_jobs:
            #     bigger_model = 'InceptionV3'
            #     smaller_model = 'ResNet50'
            #     bigger_model_vmcount = inceptionV3_running_jobs + len(free_workers)
            #     smaller_model_vmcount = resnet50_running_jobs
            # else:
            #     bigger_model = 'ResNet50'
            #     smaller_model = "InceptionV3"
            #     bigger_model_vmcount = resnet50_running_jobs + len(free_workers)
            #     smaller_model_vmcount = inceptionV3_running_jobs



            # while difference > 20 and bigger_model_vmcount >= smaller_model_vmcount:
            #     bigger_model_vmcount -=1 
            #     smaller_model_vmcount +=1
            #     bigger_model_query_rate = (bigger_model_vmcount * self.model_dict[bigger_model]['hyperparams']['batch_size']) / self.model_dict[bigger_model]['hyperparams']['time']
            #     smaller_model_query_rate = (smaller_model_vmcount * self.model_dict[smaller_model]['hyperparams']['batch_size']) / self.model_dict[smaller_model]['hyperparams']['time']

            #     difference = (abs( bigger_model_query_rate - smaller_model_query_rate ) / max(bigger_model_query_rate, smaller_model_query_rate)) * 100

ssh bachina3@fa22-cs425-6901.cs.illinois.edu
$is_e4VJL4i9a6F

mkdir MP4
cd MP4
git clone https://gitlab.engr.illinois.edu/mgoel7/awesomedml.git

python3.9 -m pip install --upgrade pip
python3.9 -m pip install tensorflow
python3.9 -m pip install Pillow

cd /home/bachina3/MP4/awesomedml/

python3.9 main.py --hostname="fa22-cs425-6910.cs.illinois.edu" --port=8000

predict-locally InceptionV3 9999 10


sshpass -p '$is_e4VJL4i9a6F' ssh bachina3@fa22-cs425-6904.cs.illinois.edu

cd MP4/awesomedml/;git remote set-url origin git@gitlab.engr.illinois.edu:mgoel7/awesomedml.git

submit-job InceptionV3 100


submit-job ResNet50 100



1/1 [==============================] - 2s 2s/step
1/1 [==============================] - 0s 320ms/step
1/1 [==============================] - 0s 317ms/step
1/1 [==============================] - 0s 317ms/step
1/1 [==============================] - 0s 315ms/step
1/1 [==============================] - 0s 318ms/step
1/1 [==============================] - 0s 316ms/step
1/1 [==============================] - 0s 316ms/step
1/1 [==============================] - 0s 317ms/step
1/1 [==============================] - 0s 318ms/step
1/1 [==============================] - 0s 315ms/step
1/1 [==============================] - 0s 318ms/step
1/1 [==============================] - 0s 317ms/step
1/1 [==============================] - 0s 317ms/step
1/1 [==============================] - 0s 315ms/step
1/1 [==============================] - 0s 317ms/step
1/1 [==============================] - 0s 318ms/step
1/1 [==============================] - 0s 318ms/step
1/1 [==============================] - 0s 315ms/step
1/1 [==============================] - 0s 317ms/step
1/1 [==============================] - 0s 317ms/step
1/1 [==============================] - 0s 315ms/step
1/1 [==============================] - 0s 317ms/step
1/1 [==============================] - 0s 320ms/step
1/1 [==============================] - 0s 317ms/step

download_time = 1 sec per image
model_load 5.6 sec
1st image 2 sec
Each image after 325ms

2022-12-03 13:40:10,137: [INFO] InceptionV3 Download of 25 images took 24.858632564544678 sec
2022-12-03 13:40:23,489: [INFO] InceptionV3 Inference on 25 downloaded images took 13.352717638015747 sec: Total runtime of task: 38.21143364906311 sec

download_time = 1 sec per image
model_load=3.5sec
first_image_predict_time=1sec
each_image_predict_time=250ms

batch_size = b

(download_time * b) + model_load + first_image_predict_time + (each_image_predict_time * (b-1))

vms_plan_to_schedule / 

6.75 sec

2022-12-03 13:47:31,663: [INFO] ResNet50 Download of 25 images took 20.67277717590332 sec
2022-12-03 13:47:41,770: [INFO] ResNet50 Inference on 25 downloaded images took 10.106865167617798 sec: Total runtime of task: 30.779736042022705 sec

New nodes for resNet50=['fa22-cs425-6908.cs.illinois.edu:8000', 'fa22-cs425-6904.cs.illinois.edu:8000', 'fa22-cs425-6905.cs.illinois.edu:8000'],[], 
New nodes for inceptionV3=[],['fa22-cs425-6910.cs.illinois.edu:8000', 'fa22-cs425-6909.cs.illinois.edu:8000', 'fa22-cs425-6907.cs.illinois.edu:8000', 'fa22-cs425-6906.cs.illinois.edu:8000', 'fa22-cs425-6903.cs.illinois.edu:8000']

"""