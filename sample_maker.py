import random,sys
from collections import defaultdict
import utils

ALPH="ABCDEFGHIJKLMNOPQRSTUVWXYZ"
PINCODE_LOW=100_000
PINCODE_HIGH=999_999
NUM_COMPANY=2000
NUM_HIGH_INTENSITY_CONN=800
NUM_LOW_INTENSITY_CONN=200

def generate_random_company(size):
  return ''.join(random.choices(ALPH,k=size))


def generate_pincode_cluster(size):
  start_index=random.randint(PINCODE_LOW,PINCODE_HIGH-size)
  return range(start_index,start_index+size-1,1)



companies=[generate_random_company(random.randint(5,10)) for _ in range(NUM_COMPANY)]

conns=defaultdict(set)
rev_conns=defaultdict(set)
  
for _ in range(NUM_HIGH_INTENSITY_CONN):
  rand_company=random.choice(companies)
  conns[rand_company].update(generate_pincode_cluster(random.randint(2,100)))

for _ in range(NUM_LOW_INTENSITY_CONN):
  rand_company=random.choice(companies)
  conns[rand_company].add(random.randint(PINCODE_LOW,PINCODE_HIGH))

for k,v_set in  conns.items():
  for v in v_set:
    rev_conns[v].add(k)

output_file="sample_input.txt"
if len(sys.argv)>1:
  output_file=sys.argv[1]
with open("answer_"+output_file,'w') as ans_f:
  with open(output_file,'w') as f:
    tot_len=sum(map(len,conns.values()))
    f.write(f"{tot_len}\n")
    for k,v_list in conns.items():
      for v in v_list:
        f.write(f"{k},{str(v).zfill(6)}\n")

    num_read_query=1000
    f.write(f"{num_read_query}\n")
    for i in range(num_read_query):
      if random.randint(0,1)==0: # Pincode
        rand_pin=random.randint(PINCODE_LOW,PINCODE_HIGH)
        f.write(f"PRINT,PIN,{rand_pin}\n")
        print(utils.get_formatted_output(rev_conns[rand_pin]),file=ans_f)
      else:
        rand_company=random.choice(companies)
        f.write(f"PRINT,COMPANY,{rand_company}\n")
        print(utils.get_formatted_output(conns[rand_company],zfill_len=6), file=ans_f)

    # num_update_query=50
    # f.write(f"{num_update_query*3}\n") # query,print1,print2
    # for i in range(num_update_query):

    #   if random.randint(0,1)==0: # add
    #     _type="ADD"
    #     rand_company=random.choice(companies)
    #     rand_pin=random.randint(PINCODE_LOW,PINCODE_HIGH)
    #     conns[rand_company].add(rand_pin)
    #     rev_conns[rand_pin].add(rand_company)
    #   else: #remove
    #     _type="REMOVE"
    #     while True:
    #       rand_company=random.choice(list(conns.keys()))
    #       if len(conns[rand_company])==0:
    #         print("hit an empty company!")
    #         continue
    #       rand_pin=random.choice(list(conns[rand_company]))
    #       break
    #     conns[rand_company].remove(rand_pin)
    #     rev_conns[rand_pin].remove(rand_company)

    #   f.write(f"{_type},{rand_company},{rand_pin}\n")
    #   f.write(f"PRINT,COMPANY,{rand_company}\n")
    #   print(utils.get_formatted_output(conns[rand_company],zfill_len=6),file=ans_f)
    #   f.write(f"PRINT,PIN,{rand_pin}\n")
    #   print(utils.get_formatted_output(rev_conns[rand_pin]),file=ans_f)
      

