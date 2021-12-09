pub mod avl_tree;


fn main() {
    println!("Hello, world!");
}

#[test]
fn test_tree() {
  let mut tree: avl_tree::Tree<i32> = avl_tree::Tree::new();
  // println!("{}", tree.show());
  tree.add(55);
  // println!("{}", tree.has(55));
  tree.add(1);
  // tree.add(100);
  tree.add(2);
  // println!("{}", tree.has(2));
  tree.add(3);
  tree.add(66);
  tree.add(101);
  tree.add(0);
  tree.add(12);
  tree.add(99);
  // println!("{}", tree.has(100));
  // println!("{}", tree.has(1000));
  // println!("{}", tree.show());
  // tree.remove(66);
  // println!("{}", tree.show());
  // tree.remove(100);
  // println!("{}", tree.show());
  // tree.remove(55);
  // println!("{}", tree.show());
  // tree.remove(1);
  // println!("{}", tree.show());
  /***********************************/
  // tree.add(99);
  // println!("removing");
  // tree.remove(99);
  // println!("wulalalala");
  /***********************************/
  // tree.add(10);
  // println!("{}", tree.height());
  // tree.add(20);
  // println!("{}", tree.height());
  // tree.add(15);
  // println!("{}", tree.height());
  println!("{}", tree.show());
}

#[test]
fn test_tree_remove() {
  let mut tree: avl_tree::Tree<i32> = avl_tree::Tree::new();
  tree.add(10);
  tree.add(5);
  tree.add(15);
  tree.add(4);
  tree.add(20);
  tree.add(8);
  tree.add(2);
  println!("{}", tree.show());
  tree.remove(5);
  tree.remove(15);
  tree.remove(10);
  println!("{}", tree.show());
}