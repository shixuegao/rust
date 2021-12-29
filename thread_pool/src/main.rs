pub mod pool;
use std::time::Duration;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use crate::pool::{Pool, Worker, Interrupt};

struct SimpleWorker {
  v: u32,
  total: Arc<AtomicU32>
}

impl SimpleWorker {
  fn new(v: u32, total: Arc<AtomicU32>) -> Self {
    SimpleWorker {v, total}
  }
}

impl Worker for SimpleWorker {
  type Output = ();

  fn work(&mut self, _: Box<dyn Interrupt>) -> Result<Self::Output, String> {    
    println!("Value is {}", self.v);
    self.total.as_ref().fetch_add(1, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(100));
    return Ok(())
  }
}

struct EchoWorker {
  v: u32,
  timeout: Duration
}

impl Worker for EchoWorker {
  type Output = u32;

  fn work(&mut self, _: Box<dyn Interrupt>) -> Result<Self::Output, String> {
    let v = (self.v + 5) * 3;
    thread::sleep(self.timeout);
    return Ok(v)
  }
}

struct PanicWorker;

impl Worker for PanicWorker {
  type Output = ();

  fn work(&mut self, _: Box<dyn Interrupt>) -> Result<Self::Output, String> {
    panic!("wualalal");
  }
}

struct InterruptWorker;

impl Worker for InterruptWorker {
  type Output = ();

  fn work(&mut self, interrupt: Box<dyn Interrupt>) -> Result<Self::Output, String> {
    println!("开始睡眠...");
    for _ in 0..10 {
      thread::sleep(Duration::from_secs(2));
      if interrupt.is_interrupted() {
        println!("被中断了...");
        break;
      }
      println!("跑起来...")
    }
    return Ok(())
  }
}

fn main() {
  
}

#[test]
fn process_test() {
  let worker_id = AtomicU32::new(0);
  let total = Arc::new(AtomicU32::new(0));
  let mut pool: Pool<SimpleWorker> = Pool::new(3, 10, Duration::from_secs(10)).unwrap();
  for _ in 0..100 {
    let c_arc = Arc::clone(&total);
    let worker = SimpleWorker::new(worker_id.fetch_add(1, Ordering::SeqCst) + 1, c_arc);
    let (p, _) = pool.process(worker);
    if let Err(s) = p.result {
      println!("Process发生异常-->{}", s);
    }
  }  
  thread::sleep(Duration::from_secs(5));
  pool.shutdown();  
  println!("total: {}", total.load(Ordering::SeqCst));
  let c_arc = Arc::clone(&total);
  let worker = SimpleWorker::new(worker_id.fetch_add(1, Ordering::SeqCst) + 1, c_arc);
  let (p, _) = pool.process(worker);
  if let Err(s) = p.result {
      println!("Process发生异常-->{}", s);
  }
  println!("退出测试");
}

#[test]
fn process_echo_test() {
  let mut pool: Pool<EchoWorker> = Pool::new(3, 10, Duration::from_secs(10)).unwrap();
  for i in 0..10 {
    let worker = EchoWorker{v: i + 1, timeout: Duration::from_secs(1)};
    let (f, _) = pool.process_echo(worker);
    let p = f(Duration::from_secs(2));
    if let Err(err) = p.result {
      println!("Process_Echo发生异常-->{}", err);
    } else {
      println!("计算完成")
    }
  }
  pool.shutdown();
  println!("退出测试");
}

#[test]
fn process_output_test() {
  let mut pool: Pool<EchoWorker> = Pool::new(3, 10, Duration::from_secs(10)).unwrap();
  for i in 0..10 {
    let worker = EchoWorker{v: i + 1, timeout: Duration::from_millis(100)};
    let (f, _) = pool.process_output(worker);
    // let (r, _, output) = f(Duration::from_secs(2));
    let p = f(Duration::from_millis(50));
    if let Err(err) = p.result {
      println!("Process_Output发生异常-->{}", err);
    } else {
      println!("计算完成, 结果为: {}", p.opt_output.unwrap());
    }
  }
  pool.shutdown();
  println!("退出测试");
}

#[test]
fn process_panic_test() {
  let mut pool: Pool<PanicWorker> = Pool::new(3, 10, Duration::from_secs(10)).unwrap();
  for _ in 0..10 {
    let worker = PanicWorker{};
    let (f, _) = pool.process_output(worker);
    // let (r, _, output) = f(Duration::from_secs(2));
    let p = f(Duration::from_millis(100));
    if let Err(err) = p.result {
      println!("Process_Output发生异常-->{}", err);
    }
    println!("剩余线程数: {}", pool.thread_count());
  }
  pool.shutdown();
  println!("退出测试");
}

#[test]
fn process_interrupt_test() {
  let mut pool: Pool<InterruptWorker> = Pool::new(3, 10, Duration::from_secs(10)).unwrap();
  for i in 0..2 {
    let worker = InterruptWorker{};
    let (_, f_interrupt) = pool.process_echo(worker);
    thread::sleep(Duration::from_secs(1));
    if i == 0 {
      f_interrupt();
    }
  }
  pool.shutdown();
  println!("退出测试");
}