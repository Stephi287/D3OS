/* ╔═════════════════════════════════════════════════════════════════════════╗
   ║ Module: scheduler                                                       ║
   ╟─────────────────────────────────────────────────────────────────────────╢
   ║ Descr.: Implementation of the scheduler.                                ║
   ╟─────────────────────────────────────────────────────────────────────────╢
   ║ Author: Fabian Ruhland, HHU                                             ║
   ╚═════════════════════════════════════════════════════════════════════════╝
*/
use crate::process::thread::{self, Thread};
use crate::{allocator, apic, scheduler, timer, tss};
use alloc::collections::btree_map::BTreeMap;
use alloc::collections::VecDeque;
use alloc::rc::Rc;
use alloc::vec::Vec;
use core::ptr;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering::Relaxed;
use smallmap::Map;
use spin::{Mutex, MutexGuard};
use log::{debug, info, warn, LevelFilter};

static THREAD_ID_COUNTER: AtomicUsize = AtomicUsize::new(1);

pub fn next_thread_id() -> usize {
    THREAD_ID_COUNTER.fetch_add(1, Relaxed)
}

/// Everything related to the ready state in the scheduler and information for eevdf 
struct ReadyState {
    initialized: bool,
    current_thread: Option<Rc<Thread>>,
    current_request: Option<Request>,
    req_tree: BTreeMap<i32,Vec<Request>>,
    virtual_time: i32,
    weight: i32, //sum of all active threads
    duration: i32, //how long is a thread supposed to work
}

impl ReadyState {
    pub fn new() -> Self { 
        Self {
            initialized: false,
            current_thread: None,
            current_request: None,
            req_tree: BTreeMap::new(),
            virtual_time: 0,
            weight: 0,
            duration: 10,
        }
    }

    pub fn update_weight(&mut self, weight: i32) {
        self.weight += weight;
    }

    /// finds a mutable request for a given thread
    pub fn find_request_for_thread_mut(&mut self, thread: &Rc<Thread>) -> Option<&mut Request> {
        let target_id = thread.id();
        self.req_tree
            .values_mut()
            .flatten()
            .find(|req| req.thread.as_ref().map(|t| t.id()) == Some(target_id))
    }
    
    /// finds a request for a given thread
    pub fn find_request_for_thread(&self, thread: &Rc<Thread>) -> Option<&Request> {
        let target_id = thread.id();
        self.req_tree
            .values()
            .flatten()
            .find(|request| request.thread.as_ref().map(|t| t.id()) == Some(target_id))
    }

    /// Tries to removes a request for a given thread and returns it if so
    pub fn remove_request_for_thread(&mut self, thread: &Rc<Thread>) -> Option<Request> {
        let target_id = thread.id();
        let mut key_to_remove = None;

        let removed_request = self.req_tree.iter_mut().find_map(|(key, requests)| {
            requests.iter().position(|req| req.thread.as_ref().map(|t| t.id()) == Some(target_id))
                .map(|pos| {
                    let removed_request = requests.remove(pos);
                    if requests.is_empty() {
                        key_to_remove = Some(*key);
                    }
                    removed_request
                })
        });

        if let Some(key) = key_to_remove {
            self.req_tree.remove(&key);
        }
        return removed_request;
    }
}

#[derive(Clone)]
struct Request {
    vd: i32,
    ve: i32,
    lag: i32,
    thread: Option<Rc<Thread>>,
    id: usize,
    sleep: bool,
}

/// Main struct of the scheduler
pub struct Scheduler {
    ready_state: Mutex<ReadyState>,
    sleep_list_eevdf: Mutex<Vec<(Request, usize)>>,
    join_map: Mutex<Map<usize, Vec<Request>>>, // manage which threads are waiting for a thread-id to terminate
}

unsafe impl Send for Scheduler {}
unsafe impl Sync for Scheduler {}

/// Called from assembly code, after the thread has been switched
#[unsafe(no_mangle)]
pub unsafe extern "C" fn unlock_scheduler() {
    unsafe { scheduler().ready_state.force_unlock(); }
}

impl Scheduler {
    /// Description: Create and init the scheduler.
    pub fn new() -> Self {
        Self {
            ready_state: Mutex::new(ReadyState::new()),
            sleep_list_eevdf: Mutex::new(Vec::new()),
            join_map: Mutex::new(Map::new()),
        }
    }

    /// Description: Called during creation of threads
    pub fn set_init(&self) {
        self.get_ready_state().initialized = true;
    }

    pub fn active_thread_ids(&self) -> Vec<usize> {
        let state = self.get_ready_state();
        let _sleep_list = self.sleep_list_eevdf.lock();
    
        state.req_tree.values()
            .flat_map(|requests| {
                requests.iter()
                    .filter(|request| !request.sleep && request.thread.is_some())
                    .map(|request| request.id)
            })
            .collect()
    }
    
    /// Description: Return reference to current thread
    pub fn current_thread(&self) -> Rc<Thread> {
        let state = self.get_ready_state();
        Scheduler::current(&state)
    }

    /// Description: Return reference to thread for the given `thread_id`
    pub fn thread(&self, thread_id: usize) -> Option<Rc<Thread>> {
        self.ready_state.lock().req_tree 
            .values()
            .flatten()
            .find(|req| req.thread.as_ref().map(|t| t.id()) == Some(thread_id)) 
            .and_then(|req| req.thread.clone()) 
    }

    /// Description: Start the scheduler, called only once from `boot.rs` 
    pub fn start(&self) {
        let mut state = self.get_ready_state();

        let next = match find_next(&state) {
            Some(value) => value,
            None => return,
        };

        state.current_thread = Some(next.clone());
        
        let req = state.find_request_for_thread(&next);
        match req {
            Some(req) =>  state.current_request = Some(req.clone()),
            None => return,
        };

        state.remove_request_for_thread(&next);

        //Initialize Accounting information for already added threads
        for request_vector in &state.req_tree {
            for request in request_vector.1 {
                for thread in &request.thread {
                    thread.inital_accounting(timer().systime_ms() as i32);
                }
            }
        }

        let next_ptr = ptr::from_ref(next.as_ref());

        unsafe { Thread::start_first(state.current_thread.as_ref().expect("Failed to dequeue first thread!").as_ref()); }
    }

    pub fn ready(&self, thread: Rc<Thread>) {
        let id = thread.id();
        let (mut state, mut join_map) = self.get_ready_state_and_join_map();
    
        join_map.insert(id, Vec::new());
    
        let request = create_request(&thread, id, &state);
        insert_request(&mut *state, &request);
        state.update_weight(1); 
  
        //for every thread that joins after start of the scheduler
        thread.inital_accounting(timer().systime_ms() as i32);
    }

    /// Description: Put calling thread to sleep for `ms` milliseconds
    pub fn sleep(&self, ms: usize) {
        let mut state = self.get_ready_state();
        let thread = Scheduler::current(&state);
        let mut current = Scheduler::current_request(&state);
        let wakeup_time = timer().systime_ms() + ms;

        if current.sleep == true {
            return;
        }

        state.weight -= 1;
        state.virtual_time += current.lag / state.weight;

        // Execute in own block, so that the lock is released automatically (block() does not return)
        {
            let mut sleep_list = self.sleep_list_eevdf.lock();
            current.sleep = true;
            current.thread.clone().unwrap().reset_acc();
            state.current_request = Some(current.clone());
            sleep_list.push((current.clone(), wakeup_time.clone()));
        }

        self.block(&mut state);
    }

    pub fn switch_thread(&self, interrupt: bool) {
        if let Some(mut state) = self.ready_state.try_lock() {

            if !state.initialized {
                return;
            }

            if let Some(mut sleep_list) = self.sleep_list_eevdf.try_lock() {
                Scheduler::check_sleep_list(&mut state, &mut sleep_list);
            }

            //Current
            let current = Scheduler::current(&state);
            let mut current_request = Scheduler::current_request(&state);
            
            // Current thread is initializing itself and may not be interrupted
            if current.stacks_locked() || tss().is_locked() {
                return;
            }

            //Accounting
            let current_time = timer().systime_ms();
            current.update_used_time(current_time as i32);
            let used_time = current.get_used_time();

            state.virtual_time += used_time;
            current_request.lag += state.duration - used_time;
            current_request.ve += used_time;
            current_request.vd = current_request.ve + state.duration;
            state.current_request = Some(current_request.clone());

            if used_time >= state.duration {
                current.reset_acc();
            }

            //next
            let next = match find_next(&state) {
                Some(value) => value,
                None => return,
            };

            //add current request back to request tree
            let request = Scheduler::current_request(&state);

            if !request.thread.is_none() {
                insert_request(&mut state, &request);
            }

            //remove next request from request tree
            let next_req = match state.remove_request_for_thread(&next) {
                Some(next_req) => next_req,
                None => return,
            };

            let current_ptr = ptr::from_ref(current.as_ref());
            let next_ptr = ptr::from_ref(next.as_ref());
       
            //Update ReadyState
            state.current_thread = Some(next.clone());
            state.current_request = Some(next_req.clone());

            if interrupt {
                apic().end_of_interrupt();
            }

            unsafe {
                Thread::switch(current_ptr, next_ptr);
            }
        }
    }

    /// Description: helper function, calling `switch_thread`
    pub fn switch_thread_no_interrupt(&self) {
        self.switch_thread(false);
    }

    /// Description: helper function, calling `switch_thread`
    pub fn switch_thread_from_interrupt(&self) {
        self.switch_thread(true);
    }

    /// 
    /// Description: Calling thread wants to wait for another thread to terminate
    /// 
    /// Parameters: `thread_id` thread to wait for
    /// 
    pub fn join(&self, thread_id: usize) {
        let mut state = self.get_ready_state();
        let thread = Scheduler::current(&state); 
        let request = Scheduler::current_request(&state); 

        {
            // Execute in own block, so that the lock is released automatically (block() does not return)
            let mut join_map = self.join_map.lock();
            let join_list = join_map.get_mut(&thread_id); 
            if join_list.is_some() {
                join_list.unwrap().push(request.clone()); 
                state.weight -= 1;
                state.virtual_time += request.lag / state.weight;
            } else {
                // Joining on a non-existent thread has no effect (i.e. the thread has already finished running)
                return;
            }
        }
        self.block(&mut state);
    }

    /// Description: Exit calling thread.
    pub fn exit(&self) {
        let mut ready_state;
        let current;

        {
            // Execute in own block, so that join_map is released automatically (block() does not return)
            let state = self.get_ready_state_and_join_map();
            ready_state = state.0;
            let mut join_map = state.1;

            current = Scheduler::current(&ready_state);
            let join_list = join_map.get_mut(&current.id()).expect("Missing join_map entry!");

            for request in join_list {
                ready_state.weight += 1;
                ready_state.virtual_time = ready_state.virtual_time.saturating_sub(request.lag / ready_state.weight);
                request.ve = ready_state.virtual_time;
                request.vd = request.ve + ready_state.duration;

                if let Some(vec_requests) = ready_state.req_tree.get_mut(&request.vd) {
                    vec_requests.push(request.clone());
                } 
                else {
                    let key = request.vd;
                    let mut vec_req  = Vec::new();
                    vec_req.push(request.clone());
                    ready_state.req_tree.insert(key, vec_req);   
                }
            }

            join_map.remove(&current.id());

            let req = Scheduler::current_request(&ready_state);
            ready_state.weight -= 1;
            ready_state.virtual_time += req.lag / ready_state.weight;
        }


        drop(current); // Decrease Rc manually, because block() does not return
        self.block(&mut ready_state);
    }

    /// 
    /// Description: Kill the thread with the  given id
    /// 
    /// Parameters: `thread_id` thread to be killed
    /// 
    pub fn kill(&self, thread_id: usize) {
        {
            // Check if current thread tries to kill itself (illegal)
            let ready_state = self.get_ready_state();
            let current = Scheduler::current(&ready_state);

            if current.id() == thread_id {
                panic!("A thread cannot kill itself!");
            }
        }

        let state = self.get_ready_state_and_join_map();
        let mut ready_state = state.0;
        let mut join_map = state.1;

        let join_list = join_map.get_mut(&thread_id).expect("Missing join map entry!");

        for request in join_list {

            if let Some(vec_requests) = ready_state.req_tree.get_mut(&request.vd) {
                vec_requests.push(request.clone());
            } 
            else {
                let key = request.vd;
                let mut vec_req  = Vec::new();
                vec_req.push(request.clone());
                ready_state.req_tree.insert(key, vec_req);   
            }
        }

        join_map.remove(&thread_id);

        let mut thread = None;
        for e in &ready_state.req_tree {
            for i in e.1 {
                if i.id == thread_id {
                    thread = i.thread.clone();
                }
            }
        }
        match thread {
            Some(thread) => {let req =ready_state.remove_request_for_thread(&thread);
                                        ready_state.weight -= 1;
                                        ready_state.virtual_time += req.unwrap().lag / ready_state.weight;},
            None => return,
        };
    }

    fn block(&self, state: &mut ReadyState) {
        {
            // Execute in own block, so that the lock is released automatically (block() does not return)
            if let Some(mut sleep_list) = self.sleep_list_eevdf.try_lock() {
                Scheduler::check_sleep_list(state, &mut sleep_list);
            }
        }
        let mut next = None;
        let mut id = 0;

        while next.is_none() {
            next = {
                //first entry in tree = lowest VD = next Request
                let mut next_req = match state.req_tree.clone().pop_first(){ 
                    Some(req) => req,
                    None => return,
                };
                //if 2 requests have the same VD choose the last
                let next_thread = match next_req.1.clone().pop(){
                    Some(req) => req,
                    None => return,
                };
                //exract thread out of request
                match &next_thread.thread {
                    Some(thread) =>{
                                                id = thread.id() as i32;
                                                Some(thread.clone())},
                    None => return,
                }
            };
        }


        let current = Scheduler::current(&state);

        let mut req = None;
        for e in &state.req_tree {
            for r in e.1 {
                if r.id as i32 == id{
                    req = Some(r.clone());
                }
            }
        }
        
        match req {
            Some(req) => state.current_request = Some(req.clone()),
            None => return,
        };

        state.current_thread = Some(next.clone().unwrap());

        state.remove_request_for_thread(&next.clone().unwrap());

        let current_ptr = ptr::from_ref(current.as_ref());
        let next_ptr = ptr::from_ref(next.unwrap().as_ref());

        drop(current); // Decrease Rc manually, because Thread::switch does not return

        unsafe {
            Thread::switch(current_ptr, next_ptr);
        }
    } 

    /// Description: Return current running thread
    fn current(state: &ReadyState) -> Rc<Thread> {
        Rc::clone(state.current_thread.as_ref().expect("Trying to access current thread before initialization!"))
    }

    /// Description: Return current request
    fn current_request(state: &ReadyState) -> Request {
        state.current_request.as_ref().expect("error").clone()
    }

    fn check_sleep_list(state: &mut ReadyState, sleep_list: &mut Vec<(Request, usize)>) {
        let current = Scheduler::current(&state);

        let time = timer().systime_ms();
        let mut expired = Vec::new();
        sleep_list.retain(|entry| {
            if time >= entry.1 {
                let mut e = entry.0.clone();
                e.sleep = false;
                expired.push(e.clone());
                false
            } else {
                true
            }
        });

        // Add requests back to request tree if awake
        for mut request in expired {
            state.weight += 1;
            state.virtual_time = state.virtual_time.saturating_sub(request.lag / state.weight);
            request.ve = state.virtual_time;
            request.vd = request.ve + state.duration;

            if let Some(vec_requests) = state.req_tree.get_mut(&request.vd) {
                vec_requests.push(request);
            } else {
                let mut requests = Vec::new();
                requests.push(request.clone());
                state.req_tree.insert(request.vd, requests);
            }
        }

        return;
    }
    

    /// Description: Helper function returning `ReadyState` of scheduler in a MutexGuard
    fn get_ready_state(&self) -> MutexGuard<ReadyState> {
        let state;

        // We need to make sure, that both the kernel memory manager and the ready queue are currently not locked.
        // Otherwise, a deadlock may occur: Since we are holding the ready queue lock,
        // the scheduler won't switch threads anymore, and none of the locks will ever be released
        loop {
            let state_tmp = self.ready_state.lock();
            if allocator().is_locked() {
                continue;
            }

            state = state_tmp;
            break;
        }

        state
    }

    /// Description: Helper function returning `ReadyState` and `Map` of scheduler, each in a MutexGuard
    fn get_ready_state_and_join_map(&self) -> (MutexGuard<ReadyState>, MutexGuard<Map<usize, Vec<Request>>>) {
        loop {
            let ready_state = self.get_ready_state();
            let join_map = self.join_map.try_lock();

            if join_map.is_some() {
                return (ready_state, join_map.unwrap());
            } else {
                self.switch_thread_no_interrupt();
            }
        }
    }
    
}

fn create_request(thread: &Rc<Thread>, id: usize, state: &MutexGuard<'_, ReadyState>) -> Request {
    let request = Request {
        ve: state.virtual_time,
        vd: state.virtual_time + state.duration, 
        lag: 0,
        thread: Some(thread.clone()),
        id: id,
        sleep: false,
    };
    request
}

fn find_next(state: &MutexGuard<'_, ReadyState>) -> Option<Rc<Thread>> {
    let len = state.req_tree.len() as i32;
    let mut req_tree = state.req_tree.clone();
    for i in 0..len {
        let next_req = match req_tree.pop_first() {
            Some(req) => req,
            None => break,
        };

        for r in next_req.1 {
            if r.lag >= 0 {
                return r.thread;
            }
        }
    };
    let req= match state.req_tree.first_key_value() {
        Some(req) => req.1.first().unwrap().thread.clone(),
        None => return None,
    };
    return req;
}

fn insert_request(state: &mut ReadyState, request: &Request) {
    if let Some(vec_requests) = state.req_tree.get_mut(&request.vd) {
        vec_requests.push(request.clone());
    } 
    else {
        //neu hinzufügen
        let key = request.vd;
        let mut vec_req  = Vec::new();
        vec_req.push(request.clone());
        state.req_tree.insert(key, vec_req);   
    }
}
