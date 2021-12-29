
use std::sync::atomic::{AtomicU32, Ordering, AtomicU64, AtomicI8};
use std::sync::mpsc::{self, Sender, Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::panic;
use std::time::{Duration, SystemTime};

const MAX_WORKER_COUNT: u32 = 1 << 31 - 1; //2^31 - 1
const POOL_STATE_RUNNING: u32 = 0;
const POOL_STATE_SHUTDOWN: u32 = 1;
const POOL_STATE_STOP: u32 = 2;

enum PoolError {
  ChannelShutdown,
  LackOfThread,
  FullWorkerQueue,
}

pub trait Interrupt {
  fn interrupt(&self) -> bool;
  fn is_interrupted(&self) -> bool;
}

pub trait Worker {
  type Output: Send;
  fn work(&mut self, interrupt: Box<dyn Interrupt>) -> Result<Self::Output, String>;
}

struct Echo<T: Worker> {
  output: Result<T::Output, String>,
}

type OptWrapperTxEcho<T> = Option<Sender<Echo<T>>>;
type OptWrapperRxEcho<T> = Option<Receiver<Echo<T>>>;

struct InnerInterrupt {
  sign: Arc<AtomicI8>
}

impl InnerInterrupt {

  fn new() -> Self {
    InnerInterrupt {
      sign: Arc::new(AtomicI8::new(0))
    }
  }

  fn copy(&self) -> Self {
    InnerInterrupt {
      sign: Arc::clone(&self.sign)
    }
  }

  fn stop(&self) {
    self.sign.store(1, Ordering::SeqCst)
  }

  fn sign_fn(opt_i: Option<InnerInterrupt>) -> impl FnOnce() -> bool {
    let f = move || {
      if let Some(i) = opt_i {
        return i.interrupt();
      }
      return false;
    };
    return f;
  }
}

impl Interrupt for InnerInterrupt {

  fn interrupt(&self) -> bool {
    return self.sign.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst) == Ok(0);
  }

  fn is_interrupted(&self) -> bool {
    return self.sign.load(Ordering::SeqCst) == 1;
  }
}

enum WrapperType {
  Normal,
  Echo,
  EchoAndOutput
}

struct Wrapper<T: Worker> {
  opt_t: Option<T>,
  w_type: WrapperType,
  tx: OptWrapperTxEcho<T>,
  interrupt: InnerInterrupt,
}

impl<T: Worker> Wrapper<T> {
  fn new(t: T, w_type: WrapperType) -> (Option<Self>, OptWrapperRxEcho<T>, Option<InnerInterrupt>) {
    let opt_w: Option<Self>;
    let interrupt = InnerInterrupt::new();
    let copy_interrupt = interrupt.copy();
    let mut opt_rx_echo: OptWrapperRxEcho<T> = None;
    match w_type {
      WrapperType::Normal => {
        opt_w = Some(Wrapper{opt_t: Some(t), w_type, tx: None, interrupt});
      }
      _ => {
        let (tx, rx) = mpsc::channel();
        opt_w = Some(Wrapper{opt_t: Some(t), w_type, tx: Some(tx), interrupt});
        opt_rx_echo = Some(rx);
      }
    }
    (opt_w, opt_rx_echo, Some(copy_interrupt))
  }
}

struct Request<T: Worker> {
  tx: Sender<Wrapper<T>>
}

impl<T: Worker> Request<T> {
  fn new(sender: &Sender<Wrapper<T>>) -> Self {
    Request { 
      tx: Sender::clone(sender)
     }
  }

  fn send(&self, w: Wrapper<T>) -> (Result<(), String>, Option<T>) {
    if let Err(err) = self.tx.send(w) {
      return (Err("发送Worker到线程失败, 线程可能已经超时或者关闭".to_string()), Some(err.0.opt_t.unwrap()));
    }
    return (Ok(()), None)
  }
}

//---------------------------------------------------

type ReqSender<T> = Sender<Request<T>>;
type ReqReceiver<T> = Receiver<Request<T>>;
type AsyncLock<T> = Arc<Mutex<Option<T>>>;

fn new_async_lock<T: Sized>(t: T) -> AsyncLock<T> {
  return Arc::new(Mutex::new(Some(t)));
}

fn async_lock_take<T: Sized>(lock: &mut AsyncLock<T>) -> Option<T> {
  let mut opt = lock.lock().unwrap();
  opt.take()
}

fn current_state(state: &AtomicU32) -> u32 {
  return state.load(Ordering::SeqCst);
}

//----------------------------------------------------

struct HandlerInner<T: Worker> {
  id: u64,
  min_size: u32,
  active_time: SystemTime,
  keep_alive_time: Duration,
  wrapper: Option<Wrapper<T>>,
  pool_state: Arc<AtomicU32>,
  workers: AsyncLock<Vec<Wrapper<T>>>,
  handlers: AsyncLock<Vec<Handler<T>>>,
  request_sender: ReqSender<T>,
  worker_sender: Sender<Wrapper<T>>,
  worker_receiver: Option<Receiver<Wrapper<T>>>,
}

impl<T: Worker> HandlerInner<T> 
{
  fn get_worker(&mut self) -> Option<Wrapper<T>> {
    if self.wrapper.is_some() {
      return self.wrapper.take()
    }
    //从队列拉取
    let opt = self.pull_worker();
    if opt.is_some() {
      return opt
    }
    //从请求拉取
    let req: Request<T> = Request::new(&self.worker_sender);
    if let Err(_) = self.request_sender.send(req) {
      return None
    }
    if let Some(t) = self.recv_worker() {
      return Some(t);
    }
    None
  }

  fn has_workers_in_queue(&self) -> bool {
    let opt = &self.workers.as_ref().lock().unwrap();
    if opt.is_some() {
      return opt.as_ref().unwrap().len() > 0;
    }
    false
  }

  fn pull_worker(&mut self) -> Option<Wrapper<T>> {
    let mut opt = self.workers.as_ref().lock().unwrap();
    if opt.is_some() && opt.as_ref().unwrap().len() > 0 {
      return opt.as_mut().unwrap().pop();
    }
    None
  }

  //最多尝试3次获取Worker, 每次间隔10ms
  fn recv_worker(&self) -> Option<Wrapper<T>> {
    for _ in 0..3 {
      if let Ok(t) = self.worker_receiver.as_ref().unwrap().try_recv() {
        return Some(t);
      }
      thread::sleep(Duration::from_millis(10));
    }
    None
  }

  fn close_channel(&mut self) {
    drop(self.worker_receiver.take());
  }

  fn update_active_time(&mut self) {
    self.active_time = SystemTime::now();
  }

  fn is_active_time_out(&self) -> bool {
    let over_time = self.active_time.checked_add(self.keep_alive_time).unwrap();
    return SystemTime::now().gt(&over_time);
  }

  fn try_quit(&mut self, force: bool) {
    let mut opt = self.handlers.as_ref().lock().unwrap();
    match *opt {
      Some(ref mut v) => {
        let cur_size = v.len();
        if !force && cur_size <= self.min_size as usize {
          return;
        }
        let mut index: usize = 0;
        for handler in v.iter() {
          if handler.id == self.id {
            v.remove(index);
            return;
          }
          index += 1;
        }
      }
      _ => {}
    }
  }
}

struct Handler<T: Worker> {
  id: u64,
  inner: Option<HandlerInner<T>>,
  join_handler: Option<JoinHandle<()>>
}

impl<T> Handler<T> 
where T: Worker + Send + 'static + panic::UnwindSafe
{
  fn new(inner: HandlerInner<T>) -> Self {
    Handler { id: inner.id, inner: Some(inner), join_handler: None }
  }

  fn new_thread(&mut self) {
    let mut inner = self.inner.take().unwrap();
    let handler = thread::spawn(move || {
      loop {
        let opt_w = inner.get_worker();
        if let Some(mut w) = opt_w {
          let t = w.opt_t.take().unwrap();
          let (r, is_panic) = Handler::deal_with_work(t, w.interrupt);
          match w.w_type {
            WrapperType::Normal => { /*do nothing*/ }
            _ => {
              let echo: Echo<T> = Echo { output: r };
              let _ = w.tx.unwrap().send(echo);
            }
          }
          if is_panic {
            inner.try_quit(true);
            break;
          }
          inner.update_active_time();
        } else {
          let cur_state = current_state(inner.pool_state.as_ref());
          if cur_state == POOL_STATE_STOP {
            break;
          } else if cur_state == POOL_STATE_SHUTDOWN {
            if inner.has_workers_in_queue() {
              continue;
            }
            break;
          } else if inner.is_active_time_out() {
            inner.try_quit(false);
            break
          }
        }
      }
      inner.close_channel();
    });
    self.join_handler = Some(handler);
  }

  fn deal_with_work(mut t: T, interrupt: InnerInterrupt) -> (Result<T::Output, String>, bool) {
    let c_interrupt = interrupt.copy();
    let r = panic::catch_unwind(move || {  return t.work(Box::new(interrupt)); });
    match r {
      Err(_) => {
        c_interrupt.stop();
        return (Err("执行任务发生Panic".to_string()), true)
      }
      Ok(inner_r) => {
        c_interrupt.stop();
        return (inner_r, false);
      }
    }
  }
}

//------------------------------------------------------------------

struct AddWorkerResult<T: Worker> {
  r: Result<(), PoolError>, 
  opt_t: Option<T>, 
  opt_rx: OptWrapperRxEcho<T>, 
  opt_interrupt: Option<InnerInterrupt>
}

impl<T: Worker> AddWorkerResult<T> {
  fn new(r: Result<(), PoolError>, opt_t: Option<T>, opt_rx: OptWrapperRxEcho<T>, opt_interrupt: Option<InnerInterrupt>) -> Self {
    return AddWorkerResult{r, opt_t, opt_rx, opt_interrupt}
  }
}

struct InnerProcessResult<T: Worker> {
  r: Result<(), String>, 
  opt_t: Option<T>, 
  opt_rx: OptWrapperRxEcho<T>, 
  opt_interrupt: Option<InnerInterrupt>
}

impl<T: Worker> InnerProcessResult<T> {
  fn new(r: Result<(), String>, opt_t: Option<T>, opt_rx: OptWrapperRxEcho<T>, opt_interrupt: Option<InnerInterrupt>) -> Self {
    return InnerProcessResult{r, opt_t, opt_rx, opt_interrupt}
  }
}

pub struct ProcessResult<T: Worker> {
  pub result: Result<(), String>,
  pub opt_t: Option<T>,
  pub opt_output: Option<T::Output>
}

impl<T: Worker> ProcessResult<T> {
  fn new(r: Result<(), String>, opt_t: Option<T>, opt_output: Option<T::Output>) -> Self {
    ProcessResult {result: r, opt_t: opt_t, opt_output: opt_output }
  }
}

pub struct Pool<T: Worker> {
  min_size: u32,
  max_size: u32,
  keep_alive_time: Duration,
  thread_id: Arc<AtomicU64>,
  state: Arc<AtomicU32>,
  workers: AsyncLock<Vec<Wrapper<T>>>,
  handlers: AsyncLock<Vec<Handler<T>>>,
  request_sender: ReqSender<T>,
  request_receiver: AsyncLock<ReqReceiver<T>>
}

impl<T> Pool<T> 
where T: Worker + Send + 'static + panic::UnwindSafe,
{
  pub fn new(min: u32, max: u32, keep_alive_time: Duration) -> Result<Self, String> {
    if max == 0 || min > max || keep_alive_time <= Duration::from_nanos(0) {
      return Err("输入的参数不合法".to_string())
    }
    let (tx, rx) = mpsc::channel();    
    let pool = Pool {
      min_size: min,
      max_size: max,
      keep_alive_time,
      thread_id: Arc::new(AtomicU64::new(0)),
      workers: new_async_lock(vec![]),
      state: Arc::new(AtomicU32::new(POOL_STATE_RUNNING)),
      handlers: new_async_lock(vec![]),
      request_sender: tx,
      request_receiver: new_async_lock(rx)
    };
    Ok(pool)
  }

  fn thread_id(&self) -> u64 {
    return self.thread_id.fetch_add(1, Ordering::SeqCst) + 1;
  }

  fn join_threads(&mut self) {
    let opt = async_lock_take(&mut self.handlers);
    match opt {
      None => {}
      Some(handlers) => {
        for handler in handlers {
          let _ = handler.join_handler.unwrap().join();
        }
      }
    }
  }

  fn add_worker(&mut self, t: T, w_type: WrapperType) -> AddWorkerResult<T> {
    //先尝试添加线程
    {
      let mut opt = self.handlers.lock().unwrap();
      if opt.is_none() {
        return AddWorkerResult::new(Err(PoolError::ChannelShutdown), Some(t), None, None);
      }
      let vec = opt.as_mut().unwrap();
      if vec.len() < self.max_size as usize {
        let (opt_w, opt_echo_rx, opt_interrupt) = Wrapper::new(t, w_type);
        let (worker_tx, worker_rx) = mpsc::channel();
        let inner = HandlerInner {
        id: self.thread_id(),
        min_size: self.min_size,
        keep_alive_time: Duration::clone(&self.keep_alive_time),
        wrapper: opt_w,
        pool_state: Arc::clone(&self.state),
        workers: Arc::clone(&self.workers),
        request_sender: Sender::clone(&self.request_sender),
        handlers: Arc::clone(&self.handlers),
        worker_sender: worker_tx,
        worker_receiver: Some(worker_rx),
        active_time: SystemTime::now(),
      };
      let mut th = Handler::new(inner);
      th.new_thread();
      vec.push(th);
      return AddWorkerResult::new(Ok(()), None, opt_echo_rx, opt_interrupt);
      }
    }
    //将worker添加到队列
    {
      let mut opt = self.workers.lock().unwrap();
      if opt.is_none() {
        return AddWorkerResult::new(Err(PoolError::ChannelShutdown), Some(t), None, None);
      } else {
        let vec = opt.as_mut().unwrap();
        if vec.len() >= MAX_WORKER_COUNT as usize {
          return AddWorkerResult::new(Err(PoolError::FullWorkerQueue), Some(t), None, None);
        }
        let (opt_w, opt_echo_rx, opt_interrupt) = Wrapper::new(t, w_type);
        vec.insert(0, opt_w.unwrap());
        return AddWorkerResult::new(Ok(()), None, opt_echo_rx, opt_interrupt);
      }
    }
  }

  fn is_running(&self) -> bool {
    return current_state(&self.state.as_ref()) == POOL_STATE_RUNNING;
  }

  fn compare_exchange(&self, current: u32, new: u32) -> bool {
    self.state.compare_exchange(current, new, Ordering::SeqCst, Ordering::SeqCst) == Ok(current)
  }

  fn close_req_sender(&mut self) {
    if let Some(recv) = async_lock_take(&mut self.request_receiver) {
      drop(recv)
    }
  }

  fn try_get_req(&mut self) -> Result<Request<T>, PoolError> {
    let mut opt = self.request_receiver.lock().unwrap();
    let receiver: &mut Receiver<Request<T>>;
    match *opt {
      None => { return Err(PoolError::ChannelShutdown) }
      Some(ref mut recv) => {
        receiver = recv;
      }
    }
    let r = receiver.try_recv();
    match r {
      Err(e) => {
        match e {
          TryRecvError::Empty => { return Err(PoolError::LackOfThread) }
          TryRecvError::Disconnected => { return Err(PoolError::ChannelShutdown) }
        }  
      }
      Ok(req) => {
        return Ok(req);
      }
    }
  }

  fn inner_process(&mut self, t: T, w_type: WrapperType) -> InnerProcessResult<T> {
    if !self.is_running() {
      return InnerProcessResult::new(Err("线程池已经关闭".to_string()), None, None, None);
    }
    let mut req = None;
    let r = self.try_get_req();
    if let Err(err) = r {
      match err {
        PoolError::ChannelShutdown => { return InnerProcessResult::new(Err("线程池已经关闭".to_string()), None, None, None); }
        _ => {}
      }
    } else if let Ok(v) = r {
      req = Some(v)
    }
    match req {
      None => {
        let awr = self.add_worker(t, w_type);
        if awr.r.is_ok() {
          return InnerProcessResult::new(Ok(()), awr.opt_t, awr.opt_rx, awr.opt_interrupt);
        } else if let Err(err) = awr.r {
          match err {
            PoolError::ChannelShutdown => { return InnerProcessResult::new(Err("线程池已经关闭".to_string()), awr.opt_t, None, None); }
            PoolError::FullWorkerQueue => { return InnerProcessResult::new(Err("线程、队列已满, 无法继续添加".to_string()), awr.opt_t, None, None)}
            _ => {}
          }
        }
      }
      Some(_) => {
        let (opt_w, opt_rx, opt_interrupt) = Wrapper::new(t, w_type);
        let (r, opt_t) = req.unwrap().send(opt_w.unwrap());
        if r.is_err() {
          return InnerProcessResult::new(r, opt_t, None, None)
        }
        return InnerProcessResult::new(Ok(()), None, opt_rx, opt_interrupt);
      }
    }
    InnerProcessResult::new(Ok(()), None, None, None)
  }

  pub fn thread_count(&self) -> usize {
    let opt = self.handlers.lock().unwrap();
    match *opt {
      None => { 0 }
      Some(ref v) => {
        v.len()
      }
    }
  }

  pub fn worker_count(&self) -> usize {
    let opt = self.workers.lock().unwrap();
    match *opt {
      None => { 0 }
      Some(ref v) => {
        v.len()
      }
    }
  }

  pub fn process(&mut self, t: T) -> (ProcessResult<T>, impl FnOnce() -> bool) {
    let ipr = self.inner_process(t, WrapperType::Normal);
    (ProcessResult::new(ipr.r, ipr.opt_t, None), InnerInterrupt::sign_fn(ipr.opt_interrupt))
  }

  pub fn process_echo(&mut self, t: T) -> (impl FnOnce(Duration) -> ProcessResult<T>, impl FnOnce() -> bool) {
    let ipr = self.inner_process(t, WrapperType::Echo);
    let f_pr = move |timeout| {
      if ipr.r.is_err() {
        return ProcessResult::new(ipr.r, ipr.opt_t, None);
      }
      let rx = ipr.opt_rx.unwrap();
      let r_echo = rx.recv_timeout(timeout);
      if let Err(err) = r_echo {
        match err {
          mpsc::RecvTimeoutError::Disconnected => {
            return ProcessResult::new(Err("回复通道已被关闭".to_string()), None, None)
          }
          mpsc::RecvTimeoutError::Timeout => {
            return ProcessResult::new(Err("操作超时".to_string()), None, None)
          }
        }
      }
      if let Err(err) = r_echo.unwrap().output {
        return ProcessResult::new(Err(err), None, None);
      }
      return ProcessResult::new(ipr.r, None, None);
    };
    let f_i = InnerInterrupt::sign_fn(ipr.opt_interrupt);
    (f_pr, f_i)
  }

  pub fn process_output(&mut self, t: T) -> (impl FnOnce(Duration) -> ProcessResult<T>, impl FnOnce() -> bool) {
    let ipr = self.inner_process(t, WrapperType::EchoAndOutput);
    let f_pr = move |timeout| {
      if ipr.r.is_err() {
        return ProcessResult::new(ipr.r, ipr.opt_t, None);
      }
      let rx = ipr.opt_rx.unwrap();
      let r_echo = rx.recv_timeout(timeout);
      if let Err(err) = r_echo {
        match err {
          mpsc::RecvTimeoutError::Disconnected => {
            return ProcessResult::new(Err("回复通道已被关闭".to_string()), None, None);
          }
          mpsc::RecvTimeoutError::Timeout => {
            return ProcessResult::new(Err("操作超时".to_string()), None, None);
          }
        }
      }
      let echo = r_echo.unwrap();
      match echo.output {
        Err(err) => {
          return ProcessResult::new(Err(err), None, None);
        }
        Ok(output) => {
          return ProcessResult::new(Ok(()), None, Some(output));
        }
      }
    };
    let f_i = InnerInterrupt::sign_fn(ipr.opt_interrupt);
    (f_pr, f_i)
  }

  pub fn process_fn(&mut self, f: impl FnOnce() -> T) -> (ProcessResult<T>, impl FnOnce() -> bool) {
    return self.process(f());
  }

  pub fn process_echo_fn(&mut self, f: impl FnOnce() -> T) -> (impl FnOnce(Duration) -> ProcessResult<T>, impl FnOnce() -> bool) {
    return self.process_echo(f());
  }

  pub fn process_output_fn(&mut self, f: impl FnOnce() -> T) -> (impl FnOnce(Duration) -> ProcessResult<T>, impl FnOnce() -> bool) {
    return self.process_output(f());
  } 

  pub fn shutdown(&mut self) {
    if !self.compare_exchange(POOL_STATE_RUNNING, POOL_STATE_SHUTDOWN) {
      return
    }
    self.close_req_sender();
    self.join_threads();
  }

  pub fn shutdown_now(&mut self) -> Option<Vec<T>> {
    if !self.compare_exchange(POOL_STATE_RUNNING, POOL_STATE_STOP) {
      return None
    }
    self.close_req_sender();
    self.join_threads();
    let mut vt: Vec<T> = vec![];
    if let Some(vw) = async_lock_take(&mut self.workers) {
      for w in vw {
        vt.push(w.opt_t.unwrap());
      }
    }
    Some(vt)
  }
}