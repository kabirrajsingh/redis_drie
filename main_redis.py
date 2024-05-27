import itertools,functools,sys,time
import utils
import redis
import numpy as np
import matplotlib.pyplot as plt

from main import DoubleTrie as RawMemoryDoubleTrie
from main import Node
  

class DoubleTrie:
  def __init__(self,redis_client:utils.RedisClient):
    self.redis_client=redis_client

  def copy_from_another_redis_db(self,new_redis_client:utils.RedisClient,trieId:int):
    self.trieId=trieId
    self.redis_client.copy_from_another_redis_db(new_redis_client,trieId)


  def load_from_file_fast(self,company_pincode_pair_list:list,trieId:int,file_name:str):
    tempDrie=RawMemoryDoubleTrie(company_pincode_pair_list)
    self.trieId=trieId

    # annotate the drie with dfs
    self.__annotate_drie_dfs(tempDrie.company_trie_head,True)
    self.__annotate_drie_dfs(tempDrie.pincode_trie_head,True)
    # add skip conns
    self.__annotate_drie_dfs(tempDrie.company_trie_head,False)
    self.__annotate_drie_dfs(tempDrie.pincode_trie_head,False)


    self.company_trie_head=tempDrie.company_trie_head._redis_id
    self.pincode_trie_head=tempDrie.pincode_trie_head._redis_id

    self.redis_client.setAttrToDB("testfilename",file_name)
    self.redis_client.setAttrToDB("trieId",self.trieId)
    self.redis_client.setAttrToDB("company_trie_head",self.company_trie_head)
    self.redis_client.setAttrToDB("pincode_trie_head",self.pincode_trie_head)

    del tempDrie

  def __annotate_drie_dfs(self,node:Node,annotate_only:bool):
    if annotate_only:
      # annotate the nodes
      parent_redis_id=None
      if node.parent:
        parent_redis_id=node.parent._redis_id

      node._redis_id=self.redis_client.createRedisNode(self.trieId,node.data,parent_redis_id,node.num_word)
    else:
      # add skip conns
      if len(node.skip_trie_trans)>0:
        self.redis_client.addSkipTrieConnOneWay(node._redis_id,[next_node._redis_id for next_node in node.skip_trie_trans])

    for next_node in node.trans.values():
      self.__annotate_drie_dfs(next_node,annotate_only)

  def load_from_file_slow(self,company_pincode_pair_list:list,trieId:int,file_name:str):
    self.trieId=trieId
    self.company_trie_head=redis_client.createRedisNode(self.trieId,"",None,0)
    self.pincode_trie_head=redis_client.createRedisNode(self.trieId,"",None,0)

    self.redis_client.setAttrToDB("testfilename",file_name)
    self.redis_client.setAttrToDB("trieId",self.trieId)
    self.redis_client.setAttrToDB("company_trie_head",self.company_trie_head)
    self.redis_client.setAttrToDB("pincode_trie_head",self.pincode_trie_head)

    # t1=time.time()
    for company,pincode in company_pincode_pair_list:
      company_leaf=self.add_string_uniq(company,Node.COMPANY_NODE)
      pincode_leaf=self.add_string_uniq(pincode,Node.PINCODE_NODE)
      # self.make_intra_trie_conns(company,pincode)
      self.redis_client.addSkipTrieConnBothWay(company_leaf,pincode_leaf)
    # t2=time.time()

    # print('Drie structure build time(seconds): ',t2-t1)
    self.optimize_conns_dfs()
  
  def load_from_db(self,):
    self.trieId=self.redis_client.getAttrFromDB("trieId")
    self.company_trie_head=self.redis_client.getAttrFromDB("company_trie_head")
    self.pincode_trie_head=self.redis_client.getAttrFromDB("pincode_trie_head")
  
  def add_string_uniq(self,string,type):
    head=self.__set_head_by_type(type)
    for i in string:
      nextNode=self.redis_client.getNextNode(head,i)
      if not nextNode: # create new node
        nextNode=self.redis_client.createRedisNode(self.trieId,i,head,0)
        nextNode=self.redis_client.getNextNode(head,i)
      head=nextNode 

    self.redis_client.setAttrForTransNode(head,"num_word","1")

    return head

  def __set_head_by_type(self,type):
    if type==Node.COMPANY_NODE:
      head=self.company_trie_head
    elif type==Node.PINCODE_NODE:
      head=self.pincode_trie_head
    
    return head
  
  def __get_node(self,string,type,raise_exception:bool=True):
    head=self.__set_head_by_type(type)
    for index,i in enumerate(string):
      head=self.redis_client.getNextNode(head,i)
      if not head:
        if not raise_exception:
          break
        raise Exception(f"char({i}) node at index({index}) of string({string}) not found in trie({type})")
        
    return head

  def exist_string_in_trie(self,string,type):
    node=self.__get_node(string,type,False)
    if node is None:
      return False
    elif int(self.redis_client.getAttrForTransNode(node,"num_word"))==0:
      return False
    else:
      return True
  
  def make_intra_trie_conns(self,company,pincode): ## need to optimize the conns elsewhere
    company_leaf=self.__get_node(company,Node.COMPANY_NODE)
    pincode_leaf=self.__get_node(pincode,Node.PINCODE_NODE)
    self.redis_client.addSkipTrieConnBothWay(company_leaf,pincode_leaf)

  def optimize_conns_node(self,node:Node):
  
    allNextNodes=list(self.redis_client.getAllNextNodes(node).values())
    if len(allNextNodes)==0:
      return 
    result_set=self.redis_client.getAllSkipConnsIntersections(allNextNodes)

    if len(result_set)==0:
      return

    for next_node in allNextNodes: # delete conns from pincode
      self.redis_client.removeFromSkipConns(next_node,result_set)
      

    # for company_node,pincode_node in itertools.product(result_set,allNextNodes): # delete conns from company
    for company_node in result_set:
      self.redis_client.removeFromSkipConns(company_node,allNextNodes)

    for company_node in result_set:
      self.redis_client.addSkipTrieConnBothWay(company_node,node)


  def optimize_conns_dfs(self,pincode_head:Node=None):
    # nodes must be processed post-order
    if not pincode_head:
      pincode_head=self.__set_head_by_type(Node.PINCODE_NODE)
    
    # for next_node in pincode_head.trans.values():
    #   self.optimize_conns_dfs(next_node)

    for data,next_node in self.redis_client.getAllNextNodes(pincode_head).items():
      self.optimize_conns_dfs(next_node)

    self.optimize_conns_node(pincode_head) 

  def get_partial_string(self,node:Node):
    ans=""
    cur=node
    while cur:
      ans+=self.redis_client.getAttrForTransNode(cur,"data")
      cur=self.redis_client.getAttrForTransNode(cur,"parent")
    ans=ans[::-1]
    return ans

  def get_all_subtree_strings(self,node:Node):
    def __DFS(node:Node,starting=False):
      full_list=[]
      for next_node in self.redis_client.getAllNextNodes(node).values():
        full_list.extend(__DFS(next_node))

      if starting:
        # return full_list
        return full_list+['']*int(self.redis_client.getAttrForTransNode(node,'num_word'))
      else:
        node_data=self.redis_client.getAttrForTransNode(node,'data')
        node_num_word=int(self.redis_client.getAttrForTransNode(node,'num_word'))
        
        return [node_data+string for string in full_list]\
        +[node_data]*node_num_word

    return __DFS(node,starting=True)

  def get_pincodes(self,company_name):
    if not self.exist_string_in_trie(company_name,Node.COMPANY_NODE):
      return []
    
    company_leaf=self.__get_node(company_name,Node.COMPANY_NODE)
    all_pincodes=[]
    for node in self.redis_client.getAllSkipConns(company_leaf): 
      pref=self.get_partial_string(node)
      substr_list=self.get_all_subtree_strings(node)

      if len(substr_list)==0: # should not come here anymore
        print("Warning!")
        all_pincodes.append(pref) 
      else:
        all_pincodes.extend(pref+string for string in substr_list)
    
    return all_pincodes
  
  def get_companies(self,pincode):
    if not self.exist_string_in_trie(pincode,Node.PINCODE_NODE):
      return []
    
    pincode_head=self.__set_head_by_type(Node.PINCODE_NODE)
    all_companies=[]
    all_companies.extend(self.get_partial_string(company_node) for company_node in self.redis_client.getAllSkipConns(pincode_head))# get from the root node

    for index,i in enumerate(pincode):
      pincode_head=self.redis_client.getNextNode(pincode_head,i)
      all_companies.extend(self.get_partial_string(company_node) for company_node in self.redis_client.getAllSkipConns(pincode_head))
      
    return all_companies

  def validate_company_pincode(self,company:str,pincode:str):
    if not self.exist_string_in_trie(company,Node.COMPANY_NODE):
      return False
    
    company_leaf=self.__get_node(company,Node.COMPANY_NODE)
    pincode_head=self.pincode_trie_head
    if company_leaf in self.redis_client.getAllSkipConns(pincode_head):
      return True
    for i in pincode:
      pincode_head=self.redis_client.getNextNode(pincode_head,i)
      if pincode_head is None:
        break
      
      if company_leaf in self.redis_client.getAllSkipConns(pincode_head):
        return True
    
    return False

  def __push_skip_conn_down_all(self,node):
    # make sure that this is not called on the leaf at all
    init_company_set=self.redis_client.getAllSkipConns(node)
    
    ## empty the skip conn set
    # node.skip_trie_trans=set()
    self.redis_client.removeFromSkipConns(node,self.redis_client.getAllSkipConns(node))
    for company_node in init_company_set:
      self.redis_client.removeFromSkipConns(company_node,[node])
    
    allNextNodes=self.redis_client.getAllNextNodes(node).values()

    for next_node in allNextNodes:
      self.redis_client.addSkipTrieConnOneWay(next_node,init_company_set)
    
    for company_node in init_company_set:
      self.redis_client.addSkipTrieConnOneWay(company_node,allNextNodes)

  def update_add_pincode(self,new_pincode):
    if self.exist_string_in_trie(new_pincode,Node.PINCODE_NODE):
      return self.__get_node(new_pincode,Node.PINCODE_NODE)
    
    pincode_head=self.pincode_trie_head
    self.__push_skip_conn_down_all(pincode_head)

    for i in new_pincode:
      pincode_head=self.redis_client.getNextNode(pincode_head,i)
      if pincode_head is None:
        break
      self.__push_skip_conn_down_all(pincode_head)
    
    return self.add_string_uniq(new_pincode,Node.PINCODE_NODE)
      
  def update_add_company(self,new_company):
    return self.add_string_uniq(new_company,Node.COMPANY_NODE)
  
  def __push_skip_conn_up_company(self,pincode_node,company_node):
    cur=pincode_node
    while cur:
      allNextNodes=self.redis_client.getAllNextNodes(cur)
      #go up if leaf node
      if len(allNextNodes)==0:
        cur=self.redis_client.getAttrForTransNode(cur,"parent")
        continue
      exist_all=True
      for next_node in allNextNodes:
        if company_node not in self.redis_client.getAllSkipConns(next_node):
          exist_all=False
          break
      if not exist_all:
        break
      #remove existing conns
      self.redis_client.removeFromSkipConns(company_node,allNextNodes)
      for next_node in allNextNodes:
        self.redis_client.removeFromSkipConns(next_node,[company_node])
      #add new conns
      self.redis_client.addSkipTrieConnBothWay(company_node,cur)
      #go up
      cur=self.redis_client.getAttrForTransNode(cur,"parent")

  def update_add_company_pincode(self,new_company,new_pincode):
    pincode_leaf=self.update_add_pincode(new_pincode)
    company_leaf=self.update_add_company(new_company)
    # self.make_intra_trie_conns(new_company,new_pincode)
    self.redis_client.addSkipTrieConnBothWay(company_leaf,pincode_leaf)

    # self.__push_skip_conn_up_company(self.__get_node(new_pincode,Node.PINCODE_NODE),self.__get_node(new_company,Node.COMPANY_NODE))
    self.__push_skip_conn_up_company(pincode_leaf,company_leaf)

  def __push_skip_conn_down_company(self,pincode_node:Node,company_leaf:Node):
    # if called on leaf node, will leak out the connection

    if company_leaf not in self.redis_client.getAllSkipConns(pincode_node):
      return 
    ## delete old conns
    # pincode_node.skip_trie_trans.remove(company_leaf)
    self.redis_client.removeFromSkipConns(pincode_node,[company_leaf])
    # company_leaf.skip_trie_trans.remove(pincode_node)
    self.redis_client.removeFromSkipConns(company_leaf,[pincode_node])
    

    allNextNodes=self.redis_client.getAllNextNodes(pincode_node).values()
    #add new conns
    for next_node in allNextNodes:
      # next_node.skip_trie_trans.add(company_leaf)
      self.redis_client.addSkipTrieConnOneWay(next_node,[company_leaf])
    # company_leaf.skip_trie_trans.update(pincode_node.trans.values()) 
    self.redis_client.addSkipTrieConnOneWay(company_leaf,allNextNodes)

  def __delete_companyleaf_from_trie(self,company_node):
    if not company_node:
      return 

    self.redis_client.setAttrForTransNode(company_node,"num_word",0)
    while len(self.redis_client.getAllNextNodes(company_node).values())==0 \
      and int(self.redis_client.getAttrForTransNode(company_node,"num_word"))==0:

      __temp_data=self.redis_client.getAttrForTransNode(company_node,"data")
      __temp_nodeId=company_node
      company_node=self.redis_client.getAttrForTransNode(company_node,"parent")
      if not company_node:
        break
      ## delete company node
      # del company_node.trans[__temp_data]
      self.redis_client.delAttrFromDB(__temp_nodeId)
      self.redis_client.delAttrForTransNode(company_node,__temp_data)


  def update_remove_company_pincode(self,old_company,old_pincode):
    if not self.validate_company_pincode(old_company,old_pincode):
      return 
    
    pincode_head=self.pincode_trie_head
    company_leaf=self.__get_node(old_company,Node.COMPANY_NODE)
    for i in old_pincode:
      self.__push_skip_conn_down_company(pincode_head,company_leaf)
      pincode_head=self.redis_client.getNextNode(pincode_head,i)
    
    #this push down will delete the conn
    self.__push_skip_conn_down_company(pincode_head,company_leaf)

    #delete the company if all conns gone
    if len(self.redis_client.getAllSkipConns(company_leaf))==0:
      self.__delete_companyleaf_from_trie(company_leaf)

    #don't delete the pincode yet
    

########################################################################
###################### Driver program ##################################
########################################################################

if __name__=="__main__":
  company_pincode_list=[]
  file_name="sample_input_noupdate.txt"
  if len(sys.argv)>1:
    file_name=sys.argv[1]
  print("testing with file: ",file_name)
  with open("answer_"+file_name) as ans_f:
    with open(file_name,'r') as f:
      lines=list(filter(lambda x:len(x)!=0,
                        map(
                          lambda x:x.strip('\r\n').strip('\n'),
                          f.readlines()
                            )
                        )
                )
    init_num=int(lines[0])
    init_company_pincode_list=list(map(lambda x:tuple(x.split(',')),lines[1:init_num+1]))
    
    redis_client=utils.RedisClient()
    drie=DoubleTrie(redis_client)
    trieId=1

    if redis_client.client.exists("testfilename") and redis_client.client.get("testfilename")==file_name:
      print("redis db already exists. Not loading.")
      drie.load_from_db()
    else:
      print("redis db not found. flushing existing DB and loading new.")
      redis_client.flushDB()
      t1=time.time()
      drie.load_from_file_fast(init_company_pincode_list,trieId,file_name)
      t2=time.time()
      print(f"done loading the db from file in {(t2-t1)} seconds!")

    messup_cnt=0
    query_ans_list=list(filter(lambda x:len(x)!=0,
                        map(
                          lambda x:x.strip('\r\n').strip('\n'),
                          ans_f.readlines()
                            )
                        )
                )
    
    print_pin_times=[]
    print_company_times=[]

    # answering queries
    num_queries=int(lines[init_num+1])
    print_q_index=0
    for query in lines[init_num+2:]:
      query=query.split(',')
      # all prints only
      if query[0]=="PRINT":
        if query[1]=="PIN":
          t1=time.time()
          l=drie.get_companies(query[2])
          t2=time.time()
          ans_s=utils.get_formatted_output(l)
          print_pin_times.append((len(l),t2-t1))
        elif query[1]=="COMPANY":
          t1=time.time()
          l=drie.get_pincodes(query[2])
          t2=time.time()
          ans_s=utils.get_formatted_output(l)
          print_company_times.append((len(l),t2-t1))

        if(query_ans_list[print_q_index]!=ans_s):
          print(f"did not match at index:{print_q_index}")
          print(f"expected: {query_ans_list[print_q_index]}")
          print(f"got: {ans_s}")
          messup_cnt+=1

        print_q_index+=1

      elif query[0]=="ADD":
        drie.update_add_company_pincode(query[1],query[2])
      
      elif query[0]=="REMOVE":
        drie.update_remove_company_pincode(query[1],query[2])

  if messup_cnt==0:
    print("All tests ran successfully!")

    # print_pin_times=np.array(print_pin_times)
    # print_company_times=np.array(print_company_times)

    # plt.figure(figsize=(10,10))

    # plt.scatter(print_pin_times[:,0],print_pin_times[:,1],label="print pin",alpha=0.5,color="red")
    # plt.legend()
    # plt.show()

    # plt.scatter(print_company_times[:,0],print_company_times[:,1],label="print company",alpha=0.5,color='blue')
    # plt.legend()
    # plt.show()
  else:
    print("Some test(s) failed!")



  # print(drie.get_pincodes("ABCD"))
  # print(drie.get_companies('7451'))
  # print(drie._DoubleTrie__get_node("",Node.PINCODE_NODE,))
  # print(drie.get_all_subtree_strings(drie.pincode_trie_head))
  # print(drie.get_all_subtree_strings(drie.company_trie_head))