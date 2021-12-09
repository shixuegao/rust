use std::fmt::Display;
use std::ptr;
use std::mem;

pub enum Rotate {
  LL,
  RR,
  LR,
  RL,
  None
}

pub enum NodeType {
  Left, 
  Right,
  None
}

impl Rotate {
  pub fn is_none(&self) -> bool {
    match self {
      Rotate::None => { true }
      _ => { false }
    }
  }
}

struct TreeNode<T>
where T: PartialEq + PartialOrd + Copy + Display + ToString
{
  elem: T,
  prev: *mut TreeNode<T>,
  left: TreeLink<T>,
  right: TreeLink<T>
}

impl<T> TreeNode<T> 
where T: PartialEq + PartialOrd + Copy + Display + ToString
{
  fn new(elem: T) -> Self {
    TreeNode { elem: elem, prev: ptr::null_mut(), left: None, right: None }
  }

  fn replace_child_return(&mut self, elem: T, new: TreeLink<T>, nt: NodeType) -> (TreeLink<T>, NodeType) {
    match nt {
      NodeType::Left => {
        return (mem::replace(&mut self.left, new), nt);
      }
      NodeType::Right => {
        return (mem::replace(&mut self.right, new), nt);
      }
      _ => {
        if let Some(ref x) = self.left {
          if x.elem == elem {
            return (mem::replace(&mut self.left, new), NodeType::Left);
          }  
        } 
        if let Some(ref x) = self.right {
          if x.elem == elem {
            return (mem::replace(&mut self.right, new), NodeType::Right);
          }
        }
      }
    }
    (None, NodeType::None)
  }

  fn reset(&mut self) {
    self.prev = ptr::null_mut();
    self.left = None;
    self.right = None;
  }

  fn search_smallest(&mut self) -> &mut TreeNode<T> {
    let mut node = self;
    loop {
      if node.left.is_none() {
        break
      } else {
        node = node.left.as_mut().unwrap().as_mut();
      }
    }
    node
  }

  fn is_balance(&self) -> bool {
    self.balance_factor().abs() <= 1
  }

  fn height(&self) -> i32 {
    let mut height = 0;
    let mut vec = vec![self];
    let mut vec2: Vec<&TreeNode<T>> = vec![];
    while vec.len() > 0 {
      height += 1;

      vec2.clear();
      for v in vec.iter() {
        if v.left.is_some() {
          vec2.push(v.left.as_ref().unwrap().as_ref())
        }
        if v.right.is_some() {
          vec2.push(v.right.as_ref().unwrap().as_ref())
        }
      }

      vec.clear();
      for v in vec2.iter() {
        vec.push(v)
      }
    }
    height
  }

  fn balance_factor(&self) -> i32 {
    let mut h_left = 0;
    let mut h_right = 0;
    if self.left.is_some() {
      h_left = self.left.as_ref().unwrap().height();
    }
    if self.right.is_some() {
      h_right = self.right.as_ref().unwrap().height();
    }
    h_left - h_right
  }

  fn rotate_type(&mut self) -> Rotate {
    let factor = self.balance_factor();
    if factor > 1 && self.left.as_ref().unwrap().balance_factor() > 0 {
      //LL
      return Rotate::LL;
    } 
    if factor < -1 && self.right.as_ref().unwrap().balance_factor() < 0 {
      //RR
      return Rotate::RR;
    }
    if factor > 1 && self.left.as_ref().unwrap().balance_factor() < 0 {
      //LR
      return Rotate::LR;
    }
    if factor < -1 && self.right.as_ref().unwrap().balance_factor() > 0 {
      //RL
      return Rotate::RL;
    }
    Rotate::None
  }
}

impl<T> Drop for TreeNode<T> 
where T: PartialEq + PartialOrd + Copy + Display + ToString
{

  fn drop(&mut self) {
    // println!("TreeNode({}) is dropped", self.elem);
  }
}

type TreeLink<T> = Option<Box<TreeNode<T>>>;

pub struct Tree<T> 
where T: PartialEq + PartialOrd + Copy + Display + ToString
{
  root: TreeLink<T>
}

impl<T> Tree<T> 
where T: PartialEq + PartialOrd + Copy + Display + ToString
{
  pub fn new() -> Self {
    Tree { root: None }
  }

  pub fn has(&self, elem: T) -> bool {
    let mut node = self.root.as_ref();
    loop {
      if node.is_none() {
        break
      }
      let tn = node.unwrap().as_ref();
      if tn.elem == elem {
        return true;
      } else if tn.elem < elem {
        node = tn.right.as_ref();
      } else {
        node = tn.left.as_ref();
      }
    }
    false
  }

  pub fn add(&mut self, elem: T) {
    let mut tn = TreeNode::new(elem);
    match self.root {
      None => {
        self.root = Some(Box::new(tn));
        return;
      }
      _ => {}
    }
    let mut node = self.root.as_mut().unwrap().as_mut();
    loop {
      if node.elem == tn.elem {
        return;
      } else if node.elem < tn.elem {
        match node.right {
          None => {
            tn.prev = node;
            let raw_tn: *mut _ = &mut tn;
            node.right = Some(Box::new(tn));
            self.balance_add(raw_tn);
            break;
          }
          Some(ref mut x) => {
            node = x.as_mut();
          }
        }
      } else {
        match node.left {
          None => {
            tn.prev = node;
            let raw_tn: *mut _ = &mut tn;
            node.left = Some(Box::new(tn));
            self.balance_add(raw_tn);
            break;
          }
          Some(ref mut x) => {
            node = x.as_mut();
          }
        }
      }
    }
  }

  pub fn remove(&mut self, elem: T) {
    if let Some(x) = self.search(elem) {
      if x.left.is_none() && x.right.is_none() {
        if x.prev.is_null() {
          x.reset();
          self.root = None;
        } else {
          unsafe {
            let prev = x.prev;
            (*prev).replace_child_return(elem, None, NodeType::None).0.unwrap().reset();
            self.balance_remove(prev);
          }
        }
      } else if x.left.is_none() {
        let mut right = x.right.take();
        if x.prev.is_null() {
          //该处self.root更改后, 后续正常流程中不能再次对x进行修改(因为x借用自self.root, 再次修改会造成多次可变借用)
          x.reset();
          self.root = right;
        } else {
          if right.is_some() {
            right.as_mut().unwrap().prev = x.prev;
          }
          unsafe {
            let prev = x.prev;
            (*prev).replace_child_return(elem, right, NodeType::None).0.unwrap().reset();
            self.balance_remove(prev);
          }
        }
      } else if x.right.is_none() {
        let mut left = x.left.take();
        if x.prev.is_null() {
          x.reset();
          self.root = left;
        } else {
          if left.is_some() {
            left.as_mut().unwrap().prev = x.prev;
          }
          unsafe {
            let prev = x.prev;
            (*prev).replace_child_return(elem, left, NodeType::None).0.unwrap().reset();
            self.balance_remove(prev);
          }
        }
      } else {
        let mut right = x.right.take();
        let mut smallest = right.as_mut().unwrap().search_smallest();
        let direct_child: bool;
        unsafe {
          direct_child = (*smallest.prev).elem == x.elem;
        }
        if direct_child {
          //right即smallest
          smallest.left = x.left.take();
          if smallest.left.is_some() {
            smallest.left.as_mut().unwrap().prev = smallest;
          }
          if x.prev.is_null() {
            x.reset();
            smallest.prev = ptr::null_mut();
            let raw_right: *mut _ = right.as_mut().unwrap().as_mut();
            self.root = right;
            self.balance_remove(raw_right);
          } else {
            smallest.prev = x.prev;
            unsafe {
              //不能直接调用smallest(其为可变借用, 会导致编译器检查时报多次可变借用), 需要将其转换为指针(指针不会纳入到借用检查器中, 大概)
              let raw_node: *mut TreeNode<T> = smallest;
              (*smallest.prev).replace_child_return(x.elem, right, NodeType::None).0.unwrap().reset();
              self.balance_remove(raw_node);
            }
          }
        } else {
          //smallest为right的子孙节点
          let mut target: Option<Box<TreeNode<T>>>;
          unsafe {
            //断开目标节点与其父节点直接的关联, 并将其右子节点树接到其父节点上
            let target_right = smallest.right.take();
            target = (*smallest.prev).replace_child_return(smallest.elem, target_right, NodeType::None).0;
            target.as_mut().unwrap().reset();
          }
          //此时的target即为smallest
          smallest = target.as_mut().unwrap();
          smallest.left = x.left.take();
          smallest.right = x.right.take();
          if smallest.left.is_some() {
            smallest.left.as_mut().unwrap().prev = smallest;
          }
          if smallest.right.is_some() {
            smallest.right.as_mut().unwrap().prev = smallest;
          }
          if x.prev.is_null() {
            smallest.prev = ptr::null_mut();
            let raw_target: *mut _ = target.as_mut().unwrap().as_mut();
            self.root = target;
            self.balance_remove(raw_target);
          } else {
            smallest.prev = x.prev;
            unsafe {
              let prev = smallest.prev;
              (*prev).replace_child_return(x.elem, target, NodeType::None).0.unwrap().reset();
              self.balance_remove(prev);
            }
          }
        }
      }
    }
    
  }

  fn search(&mut self, elem: T) -> Option<&mut TreeNode<T>> {
    match self.root {
      None => {}
      Some(ref mut node) => {
        let mut node1 = node;
        loop {
          if node1.elem == elem {
            return Some(node1)
          } else if node1.elem < elem {
            match node1.right {
              None => { break; }
              Some(ref mut x) => {
                node1 = x;
              }
            }
          } else {
            match node1.left {
              None => { break; }
              Some(ref mut x) => {
                node1 = x;
              }
            }
          }
        }
      }
    };
    None
  }
  
  pub fn height(&self) -> i32 {
    if self.root.is_none() {
      0
    } else {
      self.root.as_ref().unwrap().height()
    }
  }

  fn balance_add(&mut self, raw_node: *mut TreeNode<T>) {
    //从节点的父节点开始找, 找到第一个左右不平衡的节点, 并将该节点为根节点的树进行平衡, 即可使得整颗树平衡
    let node: &mut TreeNode<T>;
    unsafe {
      node = &mut *raw_node;
    }
    if node.prev.is_null() {
      return;
    }
    let mut node = node;  
    let mut unbalanced_node: Option<&mut TreeNode<T>> = None;
    loop {
      if node.prev.is_null() {
        break;
      }
      unsafe {
        node = &mut *node.prev;
      }
      if !node.is_balance() {
        unbalanced_node = Some(node);
        break
      }
    }
    if unbalanced_node.is_none() {
      return
    }
    self.rotate(unbalanced_node.unwrap());
  }

  fn balance_remove(&mut self, raw_node: *mut TreeNode<T>) {
    //当前节点或者第一个不平衡的父节点, 将该节点进行平衡即可
    let node: &mut TreeNode<T>;
    unsafe {
      node = &mut *raw_node;
    }
    let mut node = node;
    let mut unbalanced_node: Option<&mut TreeNode<T>> = None;
    loop {
      if !node.is_balance() {
        unbalanced_node = Some(node);
        break;
      }

      if node.prev.is_null() {
        break;
      }
      unsafe {
        node = &mut *node.prev;
      }
    }
    if unbalanced_node.is_none() {
      return;
    }
    self.rotate(unbalanced_node.unwrap());
  }

  fn rotate(&mut self, node: &mut TreeNode<T>) {
    let rotate = node.rotate_type();
    if rotate.is_none() {
      return;
    }
    //将当前node从树中分离出来
    let t: (Box<TreeNode<T>>, NodeType);
    unsafe {
      if node.prev.is_null() {
        t = (self.root.take().unwrap(), NodeType::None);
      } else {
        let (link, nt) = (*node.prev).replace_child_return(node.elem, None, NodeType::None);
        t = (link.unwrap(), nt);
      }
    }
    match rotate {
      Rotate::LL => {
        let elem = t.0.elem;
        let new_node = self.ll_rotate(t.0);
        if new_node.prev.is_null() {
          self.root = Some(new_node);
        } else {
          unsafe {
            (*new_node.prev).replace_child_return(elem, Some(new_node), t.1);
          }
        }
      }
      Rotate::RR => {
        let elem = t.0.elem;
        let new_node = self.rr_rotate(t.0);
        if new_node.prev.is_null() {
          self.root = Some(new_node);
        } else {
          unsafe {
            (*new_node.prev).replace_child_return(elem, Some(new_node), t.1);
          }
        }
      }
      Rotate::LR => {
        self.lr_rotate(t)
      }
      Rotate::RL => {
        self.rl_rotate(t)
      }
      _ => {}
    }
  }

  fn ll_rotate(&mut self, mut cur_node: Box<TreeNode<T>>) -> Box<TreeNode<T>> {
    let mut left = cur_node.left.take().unwrap();
    cur_node.left = left.right.take();
    if cur_node.left.is_some() {
      cur_node.left.as_mut().unwrap().prev = cur_node.as_mut();
    }
    if cur_node.prev.is_null() {
      left.prev = ptr::null_mut();
      cur_node.prev = left.as_mut();
      left.right = Some(cur_node);
    } else {
      left.prev = cur_node.prev;
      cur_node.prev = left.as_mut();
      left.right = Some(cur_node);
    }
    left
  }

  fn rr_rotate(&mut self, mut cur_node: Box<TreeNode<T>>) -> Box<TreeNode<T>> {
    let mut right = cur_node.right.take().unwrap();
    cur_node.right = right.left.take();
    if cur_node.right.is_some() {
      cur_node.right.as_mut().unwrap().prev = cur_node.as_mut();
    }
    if cur_node.prev.is_null() {
      right.prev = ptr::null_mut();
      cur_node.prev = right.as_mut();
      right.left = Some(cur_node);
    } else {
      right.prev = cur_node.prev;
      cur_node.prev = right.as_mut();
      right.left = Some(cur_node);
    }
    right
  }

  fn lr_rotate(&mut self, mut t:(Box<TreeNode<T>>, NodeType)) {
    let left = t.0.left.take().unwrap();
    //先左旋
    let new_left = self.rr_rotate(left);
    t.0.left = Some(new_left);
    //再右旋
    let elem = t.0.elem;
    let new_node = self.ll_rotate(t.0);
    if new_node.prev.is_null() {
      self.root = Some(new_node);
    } else {
      unsafe {
        (*new_node.prev).replace_child_return(elem, Some(new_node), t.1);
      }
    }
  }

  fn rl_rotate(&mut self, mut t:(Box<TreeNode<T>>, NodeType)) {
    let right = t.0.right.take().unwrap();
    //先右旋
    let new_right = self.ll_rotate(right);
    t.0.right = Some(new_right);
    //再左旋
    let elem = t.0.elem;
    let new_node = self.rr_rotate(t.0);
    if new_node.prev.is_null() {
      self.root = Some(new_node);
    } else {
      unsafe {
        (*new_node.prev).replace_child_return(elem, Some(new_node), t.1);
      }
    }
  }

  pub fn show(&self) -> String {
    enum Temp<'a, T> 
    where T: PartialEq + PartialOrd + Copy + Display + ToString
    {
      Separator,
      None,
      Some(&'a TreeNode<T>)
    }

    let mut s = String::from("");
    if self.root.is_some() {
      let mut vec: Vec<Temp<T>> = vec![];
      let mut vec2: Vec<&TreeNode<T>> = vec![];
      let mut level = 1;
      let root = self.root.as_ref().unwrap().as_ref();
      vec.push(Temp::Some(root));
      while vec.len() > 0 {
        let mut ss = format!("Level {} =>", level);
        for v in vec.iter() {
          match v {
            Temp::None => { ss.push_str("*") }
            Temp::Separator => { ss.push_str("/") }
            Temp::Some(x) => {
              unsafe {
                if x.prev.is_null() {
                  ss.push_str(format!(" {}({}) ", x.elem, "null").as_str());
                } else {
                  ss.push_str(format!(" {}({}) ", x.elem, (*x.prev).elem).as_str());
                }
              }
            }
          }
        }
        s.push_str(ss.as_str());
        s.push_str("\n");
        //重新装填
        level += 1;
        vec2.clear();
        for v in vec.iter() {
          match v {
            Temp::Some(x) => {
              vec2.push(*x);
            }
            _ => {}
          }
        }
        vec.clear();
        for v in vec2.iter() {
          vec.push(Temp::Separator);
          if v.left.is_some() {
            vec.push(Temp::Some(v.left.as_ref().unwrap().as_ref()));
          } else {
            vec.push(Temp::None);
          }
          if v.right.is_some() {
            vec.push(Temp::Some(v.right.as_ref().unwrap().as_ref()));
          } else {
            vec.push(Temp::None)
          }
        }
      }
    }
    s
  }
}