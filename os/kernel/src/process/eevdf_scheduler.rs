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

// thread IDs
static THREAD_ID_COUNTER: AtomicUsize = AtomicUsize::new(1); //der erste Thread hat die ID = 1 

pub fn next_thread_id() -> usize {
    THREAD_ID_COUNTER.fetch_add(1, Relaxed) //fetch_add = ID um 1 erhöhen und returnen
}

/// Everything related to the ready state in the scheduler
//Threads die ready sind und warten vom Scheduler eingeteilt zu werden
struct ReadyState {
    initialized: bool, //ob ReadyState korrekt initialisiert ist
    current_thread: Option<Rc<Thread>>, //kein Wert oder geteilter Zeiger auf current_thread
    ready_queue: VecDeque<Rc<Thread>>, //kein Wert oder geteilter Zeiger auf ready_queue
    req_tree: BTreeMap<i32,Vec<Request>>, 
    virtual_time: i32,
    weight: i32,
}

impl ReadyState {
    pub fn new() -> Self { //quasi Konstruktor für ReadyState
        Self {
            initialized: false,
            current_thread: None,
            ready_queue: VecDeque::new(),
            req_tree: BTreeMap::new(),
            virtual_time: 0,
            weight: 0,
        }
    }

    pub fn update_virtual_time(&mut self, time: i32) {
        self.virtual_time += time;
    }

    pub fn update_weight(&mut self, weight: i32) {
        self.weight += weight;
    }

    //wird nur einmal benutzt
    pub fn find_request_for_thread_mut(&mut self, thread: &Rc<Thread>) -> Option<&mut Request> { 
        let target_id = thread.id(); 
        for (_vd, requests) in self.req_tree.iter_mut() { 
            if let Some(request) = requests.iter_mut()
            .find(|req| req.thread.as_ref()
            .map(|t| t.id()) == Some(target_id)) { 
                return Some(request); 
            } 
        } 
        None 
    }

    //wird nur einmal benutzt
     /// Sucht in der BTreeMap `req_tree` nach dem Request, der zu `thread` gehört.
     pub fn find_request_for_thread(&self, thread: &Rc<Thread>) -> Option<&Request> {
        let target_id = thread.id();
        // Durchlaufe alle Einträge in der BTreeMap
        for (_vd, requests) in self.req_tree.iter() {
            // Durchsuche den Vektor nach dem Request
            for request in requests {
                if let Some(ref req_thread) = request.thread {
                    if req_thread.id() == target_id {
                        return Some(request);
                    }
                }
            }
        }
        // Falls kein Request gefunden wurde, None zurückgeben.
        None
    }

    /// Entfernt den Request, der zum gegebenen `thread` gehört, aus der `req_tree`.
    /// Gibt den entfernten Request zurück, falls vorhanden.
    pub fn remove_request_for_thread(&mut self, thread: &Rc<Thread>) -> Option<Request> {
        let target_id = thread.id();

        // Erstelle eine Kopie aller Schlüssel, damit wir während der Iteration die Map modifizieren können.
        let keys: Vec<_> = self.req_tree.keys().cloned().collect();

        // Iteriere über alle Schlüssel der BTreeMap.
        for key in keys {
            // Hole einen veränderlichen Zugriff auf den Vektor der Requests für den aktuellen Schlüssel.
            if let Some(requests) = self.req_tree.get_mut(&key) {
                // Suche nach der Position des Requests, dessen Thread-ID mit target_id übereinstimmt.
                if let Some(pos) = requests.iter().position(|req| {
                    req.thread.as_ref().map(|t| t.id()) == Some(target_id)
                }) {
                    // Entferne den Request aus dem Vektor.
                    let removed_request = requests.remove(pos);
                    
                    // Wenn der Vektor nach dem Entfernen leer ist, entferne auch den Schlüssel aus der Map.
                    if requests.is_empty() {
                        self.req_tree.remove(&key);
                    }
                    
                    return Some(removed_request);
                }
            }
        }
        // Falls kein passender Request gefunden wurde, gebe None zurück.
        None
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
        // Zustand holen und sleep_list sperren, um Konsistenz zu gewährleisten
        let state = self.get_ready_state();
        let _sleep_list = self.sleep_list_eevdf.lock();
    
        // Alle aktiven Threads (nicht schlafende Requests mit Some(thread)) sammeln
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
        self.ready_state.lock().req_tree //ready state locken, um aktuellen Thread zu finden
            .values()
            .flatten()
            .find(|req| req.thread.as_ref().map(|t| t.id()) == Some(thread_id)) // Den passenden Request finden
            .and_then(|req| req.thread.clone()) // Den Thread aus dem Request zurückgeben
    }

    /// Description: Start the scheduler, called only once from `boot.rs` 
    pub fn start(&self) {
        let mut state = self.get_ready_state();

        //das als methode?
        let next ={
            let next_req = match state.req_tree.first_key_value() { //neuen raus nehmen
                Some(req) => req,
                None => return,
            };
            //falls 2 Requests dieselbe VD haben, den ersten nehmen
            let next_thread = match next_req.1.first(){
                Some(req) => req,
                None => return,
            };
            //Thread aus dem Request holen
            match &next_thread.thread {
                Some(thread) => thread.clone(),
                None => return,
            }
        };

        state.current_thread = Some(next.clone()); //ersten Thread der rechnen soll auswählen
        
        let req = state.find_request_for_thread(&next);
        match req {
            Some(req) =>  state.current_request = Some(req.clone()),
            None => return,
        };

        state.remove_request_for_thread(&next);

        let next_ptr = ptr::from_ref(next.as_ref());

        unsafe { Thread::start_first(state.current_thread.as_ref().expect("Failed to dequeue first thread!").as_ref()); }
    }

    pub fn ready(&self, thread: Rc<Thread>) {
        let id = thread.id();
        let (mut state, mut join_map) = self.get_ready_state_and_join_map();
    
        join_map.insert(id, Vec::new());
    
        // EEVDF: Erhöhe Gewicht und füge den neuen Request in den Baum ein
        state.update_weight(1);
        let request = Request {
            ve: state.virtual_time,
            vd: state.virtual_time + 10,
            lag: 0,
            thread: Some(thread.clone()),
            id: id,
            sleep: false,
        };
    
        if let Some(vec_requests) = state.req_tree.get_mut(&request.vd) {
            vec_requests.push(request);
        } else {
            let key = request.vd;
            let mut vec_req = Vec::new();
            vec_req.push(request);
            state.req_tree.insert(key, vec_req);
        }
        let current_time = timer().systime_ms() as i32;
        thread.inital_accounting(current_time);
    }

    /// Description: Put calling thread to sleep for `ms` milliseconds
    pub fn sleep(&self, ms: usize) {
        let mut state = self.get_ready_state();
        let thread = Scheduler::current(&state);
        let mut request = Scheduler::current_request(&state);
        let wakeup_time = timer().systime_ms() + ms;

        if request.sleep == true {
            return;
        }

        if let Some(request2) = state.remove_request_for_thread(&thread) {
            state.update_weight(-1);
            if state.weight > 0 {
                state.virtual_time += request2.lag / state.weight;
            }
        }

        //state.virtual_time += request.lag / state.weight;
            
        // Execute in own block, so that the lock is released automatically (block() does not return)
        {
            let mut sleep_list = self.sleep_list_eevdf.lock();
            request.sleep = true;
            state.current_request = Some(request.clone());
            sleep_list.push((request.clone(), wakeup_time));
        }

            self.block_eevdf(&mut state);
    }

    pub fn next_request(&self, interrupt: bool) {
        if let Some(mut state) = self.ready_state.try_lock() {

            if !state.initialized {
                return;
            }

            if let Some(mut sleep_list) = self.sleep_list_eevdf.try_lock() {
                Scheduler::check_sleep_list_eevdf2(&mut state, &mut sleep_list);
            }

            //Current
            let current = Scheduler::current(&state);
            
            // Current thread is initializing itself and may not be interrupted
            if current.stacks_locked() || tss().is_locked() {
                return;
            }

            //Accounting
            let x = current.get_accounting();

            let current_time = timer().systime_ms();

            state.virtual_time += x;

            current.update_accounting(current_time as i32);

            if x >= 10 {
                current.reset_acc();
            }
            else {
                //////debug!("FAIL Updated thread runtime: {} for ID {}", x, current.id());
            }

            //next
            let next = {
                //erster Eintrag im Baum = niedrigste VD = nächster Request
                let next_req = match state.req_tree.first_key_value(){ //ersten auslesen
                    Some(req) => req,
                    None => return,
                };
                //falls 2 Requests dieselbe VD haben, den ersten nehmen
                let next_thread = match next_req.1.first(){
                    Some(req) => req,
                    None => return,
                };
                //Thread aus dem Request holen
                match &next_thread.thread {
                    Some(thread) => thread.clone(),
                    None => return,
                }
            };

            //current request wieder einfügen
            let request = Scheduler::current_request(&state);

            if !request.thread.is_none() {
                insert_request(&mut state, &request);
            }

            //next request entfernen
            let next_req = match state.remove_request_for_thread(&next) {
                Some(next_req) => next_req,
                None => return,
            };

            //Pointer auf aktuellen und nächsten Thread finden
            let current_ptr = ptr::from_ref(current.as_ref());
            let next_ptr = ptr::from_ref(next.as_ref());
       
            //aktuellen Thread auf nächsten setzen im ReadyState
            state.current_thread = Some(next.clone());
            state.current_request = Some(next_req.clone());

            //Berechnung der neuen vd ?
            //Berechnung von lag -> wie lang wurde tatsächlich gerechnet
            //state.req_tree.insert(next_thread.vd, next_req.1); //alten wieder rein tun

            if interrupt {
                apic().end_of_interrupt();
            }

            //debug!("cur pointer: {}, next pointer: {}", current_ptr as i32, next_ptr as i32);

            //debug!("ENDE NEXT REQUEST");

            unsafe {
                Thread::switch(current_ptr, next_ptr);
            }
        }
    }

    /// Description: helper function, calling `switch_thread`
    pub fn switch_thread_no_interrupt(&self) {
        self.next_request(false);
    }

    /// Description: helper function, calling `switch_thread`
    pub fn switch_thread_from_interrupt(&self) {
        self.next_request(true);
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

        //debug!("current ist {} und übergebene id ist {}", thread.id() as i32, thread_id as i32); //z.b. current = 2 und thread_id = 5

        {
            // Execute in own block, so that the lock is released automatically (block() does not return)
            let mut join_map = self.join_map.lock();
            let join_list = join_map.get_mut(&thread_id); //Warteliste für Threads mit id 5 holen
            if join_list.is_some() {
                join_list.unwrap().push(request); //thread 2 in die Warteliste packen

            } else {
                // Joining on a non-existent thread has no effect (i.e. the thread has already finished running)
                return;
            }
        }

        self.block_eevdf(&mut state);
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
                if let Some(vec_requests) = ready_state.req_tree.get_mut(&request.vd) {
                    vec_requests.push(request.clone());
                } 
                else {
                    //neu hinzufügen
                    let key = request.vd;
                    let mut vec_req  = Vec::new();
                    vec_req.push(request.clone());
                    ready_state.req_tree.insert(key, vec_req);   
                }
            }
            ////debug!("Exit Thread {}", current.id());
            join_map.remove(&current.id());
        }

        drop(current); // Decrease Rc manually, because block() does not return
        self.block_eevdf(&mut ready_state);
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
            ////debug!("Kill Thread {}", current.id());
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
                //neu hinzufügen
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
            Some(thread) => ready_state.remove_request_for_thread(&thread),
            None => return,
        };
    }

    fn block_eevdf(&self, state: &mut ReadyState) {
        ////debug!("BEGINN BLOCK");
        {
            // Execute in own block, so that the lock is released automatically (block() does not return)
            if let Some(mut sleep_list) = self.sleep_list_eevdf.try_lock() {
                Scheduler::check_sleep_list_eevdf2(state, &mut sleep_list);
            }
            //let mut sleep_list = self.sleep_list_eevdf.lock();
            //Scheduler::check_sleep_list_eevdf2(state, &mut sleep_list);
        
        }
        let mut next = None;
        let mut id = 0;

        for e in &state.req_tree {
            for i in e.1 {
                ////debug!("in block vor next auswahl {}", i.id);
            }
        }

        //wird immer ein Thread gefunden?
        while next.is_none() {
            next = {
                //erster Eintrag im Baum = niedrigste VD = nächster Request
                let mut next_req = match state.req_tree.clone().pop_first(){ //neuen raus nehmen
                    Some(req) => req,
                    None => return,
                };
                //falls 2 Requests dieselbe VD haben, den ersten nehmen
                let next_thread = match next_req.1.clone().pop(){
                    Some(req) => req,
                    None => return,
                };
                //Thread aus dem Request holen
                match &next_thread.thread {
                    Some(thread) =>{
                                                ////debug!("next id {}", thread.id() as i32);
                                                id = thread.id() as i32;
                                                Some(thread.clone())},
                    None => return,
                }
            };
        }

        for e in &state.req_tree {
            for i in e.1 {
                ////debug!("in block nach next auswahl {}", i.id);
            }
        }

        let current = Scheduler::current(&state);

        ////debug!("current id in block {}", current.id() as i32);
        

        let current_ptr = ptr::from_ref(current.as_ref());
        let next_ptr = ptr::from_ref(next.clone().unwrap().as_ref());

        let mut req = None;
        for e in &state.req_tree {
            for r in e.1 {
                ////debug!("id: {}", r.id);
                if r.id as i32 == id{
                    req = Some(r.clone());
                    ////debug!("Zu next id {} gehört req mit id {}", id, r.id  as i32);
                }
            }
        }
        
        match req {
            Some(req) => state.current_request = Some(req.clone()),
            None => {////debug!("zughörigen request nicht gefunden");
                return},
        };

        state.remove_request_for_thread(&next.clone().unwrap());


        if Some(next.clone()).is_none() {
            panic!("BLOCK: No thread to schedule!");
        }
        state.current_thread = Some(next.unwrap());

        drop(current); // Decrease Rc manually, because Thread::switch does not return

        //debug!("BLOCK: cur pointer: {}, next pointer: {}", current_ptr as i32, next_ptr as i32);

        unsafe {
            Thread::switch(current_ptr, next_ptr);
        }
    } 

    /// Description: Return current running thread
    fn current(state: &ReadyState) -> Rc<Thread> {
        Rc::clone(state.current_thread.as_ref().expect("Trying to access current thread before initialization!"))
    }

    /// Description: Return current running thread
    fn current_request(state: &ReadyState) -> Request {
        state.current_request.as_ref().expect("error").clone()
    }

    fn check_sleep_list_eevdf2(state: &mut ReadyState, sleep_list: &mut Vec<(Request, usize)>) {
        let current = Scheduler::current(&state);

        let time = timer().systime_ms();
        // Sammle alle abgelaufenen Requests in einem temporären Vektor
        let mut expired = Vec::new();
        sleep_list.retain(|entry| {
            if time >= entry.1 {
                let mut e = entry.0.clone();
                e.sleep = false;
                expired.push(e.clone());
                false // Entferne abgelaufene Einträge
            } else {
                //debug!("Wacht auf um {}", entry.1 as i32);
                true
            }
        });

        // Füge alle abgelaufenen Requests in den Request-Baum ein
        for request in expired {
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
