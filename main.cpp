#include <bits/stdc++.h>
using namespace std;

template<class T>
struct Node{
  T data;
  map<T,Node*> transitions;
};

template <class T>
class trie
{
private:
  vector<T> hold;
  int hold_size;
  int found;
  int _size;
  struct node
  {
    map<T, node *> link;
    int ending;
    node() : link(map<T, node *>()), ending(0)
    {
    }
    ~node()
    {
    }
  };
  node root;
  void __traverse(node *const p, int index)
  {
    if (index == hold_size)
    {
      // if(p->ending>0)
      //   found=;
      found = p->ending;
      return;
    }
    // if(hold[index]=='.')
    // {
    //   for(auto& x:p->link)
    //   {
    //     traverse(x.second,index+1);
    //     if(found)
    //       return ;
    //   }
    // }
    // else
    // {
    auto it = p->link.find(hold[index]);
    if (it != p->link.end())
    {
      __traverse(it->second, index + 1);
    }
    // }
  }
  bool __del_traverse(node *const p, int index)
  {
    if (index == hold_size)
    {
      if (!p->ending)
      {
        --p->ending;
        return 1;
      }
      return 0;
    }
    auto it = p->link.find(hold[index]);
    if (it != p->link.end())
    {
      return __del_traverse(it->second, index + 1);
    }
    return 0;
  }
  void __dfs_clear(node *const p)
  {
    for (auto &x : p->link)
    {
      __dfs_clear(x.second);
      delete x.second;
    }
  }

public:
  trie() : _size(0)
  {
  }
  void add_stuff(const vector<T> &v)
  {
    hold.assign(v.begin(), v.end());
    hold_size = v.size();
    int index = 0;
    node *cur = &root;
    while (index != hold_size)
    {
      auto it = cur->link.find(hold[index]);
      if (it == cur->link.end())
      {
        // test(1);
        cur->link[hold[index]] = new node();
        // cur=cur->link[hold[index]];
      }
      cur = cur->link[hold[index]];
      ++index;
    }
    ++(cur->ending);
    ++_size;
  }
  int find_stuff(const vector<T> &v)
  {
    found = 0;
    hold.assign(v.begin(), v.end());
    hold_size = v.size();
    __traverse(&root, 0);
    return found;
  }
  bool delete_stuff(const vector<T> &v)
  {
    hold.assign(v.begin(), v.end());
    hold_size = v.size();
    auto val = __del_traverse(&root, 0);
    if (val)
      --_size;
    return val;
  }
  void clear()
  {
    __dfs_clear(&root);
    root.link.clear();
    _size = 0;
  }
  int size()
  {
    return _size;
  }
  ~trie()
  {
    clear();
  }
};

int main(){
  
}