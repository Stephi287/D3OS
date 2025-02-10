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
    sleep_list_eevdf: Vec<(Request, usize)>,
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
            sleep_list_eevdf: Vec::new(),
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

    /// Sucht in der BTreeMap `req_tree` nach dem Request, der zu `thread` gehört.
    pub fn find_request_for_thread(&self, thread: &Rc<Thread>) -> Option<Request> {
        let target_id = thread.id();
        // Durchlaufe alle Einträge in der BTreeMap
        for (_vd, requests) in self.req_tree.iter() {
            // Durchsuche den Vektor nach dem Request
            for request in requests {
                if let Some(ref req_thread) = request.thread {
                    if req_thread.id() == target_id {
                        return Some(request.clone());
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
}

/// Main struct of the scheduler
pub struct Scheduler {
    ready_state: Mutex<ReadyState>,
    sleep_list: Mutex<Vec<(Rc<Thread>, usize)>>,
    join_map: Mutex<Map<usize, Vec<Rc<Thread>>>>, // manage which threads are waiting for a thread-id to terminate
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
            sleep_list: Mutex::new(Vec::new()),
            join_map: Mutex::new(Map::new()),
        }
    }

    /// Description: Called during creation of threads
    pub fn set_init(&self) {
        self.get_ready_state().initialized = true;
    }

    pub fn active_thread_ids(&self) -> Vec<usize> {
        let state = self.get_ready_state();
        let sleep_list = self.sleep_list.lock();

        state.ready_queue.iter()
            .map(|thread| thread.id())
            .collect::<Vec<usize>>()
            .into_iter()
            .chain(sleep_list.iter().map(|entry| entry.0.id()))
            .collect()
    }

    /// Description: Return reference to current thread
    pub fn current_thread(&self) -> Rc<Thread> {
        let state = self.get_ready_state();
        Scheduler::current(&state)
    }

    /// Description: Return reference to thread for the given `thread_id`
    pub fn thread(&self, thread_id: usize) -> Option<Rc<Thread>> {
        self.ready_state.lock().ready_queue
            .iter()
            .find(|thread| thread.id() == thread_id)
            .cloned()
    }

    /// Description: Start the scheduler, called only once from `boot.rs` 
    pub fn start(&self) {
        let mut state = self.get_ready_state();
        state.current_thread = state.ready_queue.pop_back();

        //Hat Einfügen in den Baum geklappt?
        let l = state.req_tree.len();
        for r in &state.req_tree {
            for v in r.1 {
                //debug!("Request eingefügt mit vd {}", v.vd);
                debug!("Request {} eingefügt", v.id as i32);
            }
        }
        debug!("Tree Size: {}", l as i32);

        unsafe { Thread::start_first(state.current_thread.as_ref().expect("Failed to dequeue first thread!").as_ref()); }
    }

    /// 
    /// Description: Insert a thread into the ready_queue
    /// 
    /// Parameters: `thread` thread to be inserted.
    /// 
    pub fn ready(&self, thread: Rc<Thread>) {
        let id = thread.id();
        let mut join_map;
        let mut state;
        let thread2 = thread.clone();

        // If we get the lock on 'self.state' but not on 'self.join_map' the system hangs.
        // The scheduler is not able to switch threads anymore, because of 'self.state' is locked,
        // and we will never be able to get the lock on 'self.join_map'.
        // To solve this, we need to release the lock on 'self.state' in case we do not get
        // the lock on 'self.join_map' and let the scheduler switch threads until we get both locks.
        loop {
            let state_mutex = self.get_ready_state();
            let join_map_option = self.join_map.try_lock();

            if join_map_option.is_some() {
                state = state_mutex;
                join_map = join_map_option.unwrap();
                break;
            } else {
                self.switch_thread_no_interrupt();
            }
        }

        state.ready_queue.push_front(thread);
        join_map.insert(id, Vec::new());

        //EEVDF
        state.update_virtual_time(10);
        state.update_weight(1);

        //Einfügen des Threads als Request in die BTreeMap
        let request = Request {
            ve: state.virtual_time,
            vd: state.virtual_time + 10,
            lag: 0,
            thread: Some(thread2),
            id: next_thread_id(),
        };

        //Key bereits vorhanden
        if let Some(vec_requests) = state.req_tree.get_mut(&request.vd) {
            vec_requests.push(request);
        } 
        else {
            //neu hinzufügen
            let key = request.vd;
            let mut vec_req  = Vec::new();
            vec_req.push(request);
            state.req_tree.insert(key, vec_req);   
        }
    
          
    }

    ///Thread rejoins after sleeping
    /* pub fn re_join(&self, thread: Rc<Thread>) {
        let mut state;
        let thread2 = thread.clone();

        // If we get the lock on 'self.state' but not on 'self.join_map' the system hangs.
        // The scheduler is not able to switch threads anymore, because of 'self.state' is locked,
        // and we will never be able to get the lock on 'self.join_map'.
        // To solve this, we need to release the lock on 'self.state' in case we do not get
        // the lock on 'self.join_map' and let the scheduler switch threads until we get both locks.
        loop {
            let state_mutex = self.get_ready_state();
            let join_map_option = self.join_map.try_lock();

            if join_map_option.is_some() {
                state = state_mutex;
                join_map = join_map_option.unwrap();
                break;
            } else {
                self.switch_thread_no_interrupt();
            }
        }
        //EEVDF
        state.update_virtual_time(10);
        state.update_weight(1);
        //Request einfügen

    } */

    /// Description: Put calling thread to sleep for `ms` milliseconds
    pub fn sleep(&self, ms: usize) {
        let mut state = self.get_ready_state();
        let thread = Scheduler::current(&state);
        let wakeup_time = timer().systime_ms() + ms;

        {
            //EEVDF
            //1 Thread weniger aktiv -> weight reduzieren
            state.update_weight(-1);

            if let Some(request) = state.remove_request_for_thread(&thread) {
                state.sleep_list_eevdf.push((request, ms));
            }

            /* 
            req_tree: map: key value: vd Vec<Request>
            Request: thread

            sleep: put current to sleep for x ms
            request anhand von thread aus req_tree entfernen
            request in sleep_tree einfügen
            -> sleep_tree kann auch sleep_list sein
            sleep_list: list: Tupel: (Vec<Request>, x)
            */

            // Execute in own block, so that the lock is released automatically (block() does not return)
            let mut sleep_list = self.sleep_list.lock();
            debug!("Thread {} schläft für {}ms", thread.id(), ms);
            sleep_list.push((thread, wakeup_time));
            //debug!("Thread schläft ab {}", timer().systime_ms() as i32);  
        }

        self.block(&mut state);
    }

    /// 
    /// Description: Switch from current to next thread (from ready queue)
    /// 
    /// Parameters: `interrupt` true = called from ISR -> need to send EOI to APIC
    ///                         false = no EOI needed
    /// 
    fn switch_thread(&self, interrupt: bool) { //wird nur über helper_fn aufgerufen
        if let Some(mut state) = self.ready_state.try_lock() {
            if !state.initialized {
                return;
            }

            if let Some(mut sleep_list) = self.sleep_list.try_lock() {
                Scheduler::check_sleep_list(&mut state, &mut sleep_list);
            }

            let current = Scheduler::current(&state);
            let next = match state.ready_queue.pop_back() {
                Some(thread) => thread,
                None => return,
            };

            // Current thread is initializing itself and may not be interrupted
            if current.stacks_locked() || tss().is_locked() {
                return;
            }

            let current_ptr = ptr::from_ref(current.as_ref());
            let next_ptr = ptr::from_ref(next.as_ref());

            //debug!("Runtime: {}", current.get_accounting());

            state.current_thread = Some(next);
            state.ready_queue.push_front(current);

            if interrupt {
                apic().end_of_interrupt();
            }

            unsafe {
                Thread::switch(current_ptr, next_ptr);
            }
        }
    }

    pub fn next_request(&self, interrupt: bool) {
        if let Some(mut state) = self.ready_state.try_lock() {
            let mut state = self.get_ready_state();

            if !state.initialized {
                return;
            }

            //aufgewachte Threads in den Request-Tree schieben
            Scheduler::check_sleep_list_eevdf(&mut state);

            //wer ist aktueller Thread?
            let current = Scheduler::current(&state);

            //erster Eintrag im Baum = niedrigste VD = nächster Request
            let next_req = match state.req_tree.pop_first() { //neuen raus nehmen
                Some(req) => req,
                None => return,
            };
            //falls 2 Requests dieselbe VD haben, den ersten nehmen
            let next_thread = match next_req.1.first(){
                Some(req) => req,
                None => return,
            };
            //Thread aus dem Request holen
            let next = match &next_thread.thread {
                Some(thread) => thread,
                None => return,
            };

            // Current thread is initializing itself and may not be interrupted
            if current.stacks_locked() || tss().is_locked() {
                return;
            }
            //Pointer auf aktuellen und nächsten Thread finden
            let current_ptr = ptr::from_ref(current.as_ref());
            let next_ptr = ptr::from_ref(next.as_ref());

            //aktuellen Thread auf nächsten setzen im ReadyState
            state.current_thread = Some(next.clone());

            //Berechnung der neuen vd ?
            //Berechnung von lag -> wie lang wurde tatsächlich gerechnet
            state.req_tree.insert(next_thread.vd, next_req.1); //alten wieder rein tun

            if interrupt {
                apic().end_of_interrupt();
            }

            //eigentliches Switchen des Threads
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

        {
            // Execute in own block, so that the lock is released automatically (block() does not return)
            let mut join_map = self.join_map.lock();
            let join_list = join_map.get_mut(&thread_id);
            if join_list.is_some() {
                join_list.unwrap().push(thread);
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

            for thread in join_list {
                ready_state.ready_queue.push_front(Rc::clone(thread));
            }
            debug!("Exit Thread {}", current.id());
            join_map.remove(&current.id());
        }

        //EEVDF
        let thread_clone = current.clone();

        ready_state.update_virtual_time(-10);
        ready_state.update_weight(-1);

        let target_id = thread_clone.id();
        let keys: Vec<_> = ready_state.req_tree.keys().cloned().collect();

        for key in keys {
            // Mit get_mut holen wir uns einen veränderlichen Zugriff auf den Vektor der Requests.
            if let Some(vec_requests) = ready_state.req_tree.get_mut(&key) {
                // Suchen nach dem Index des Requests, der zum Thread gehört.
                if let Some(pos) = vec_requests.iter()
                    .position(|req| req.thread.as_ref().map(|t| t.id()) == Some(target_id))
                {
                    // Entfernen des Requests aus dem Vektor.
                    let removed_request = vec_requests.remove(pos);
                    
                    // Falls der Vektor nach dem Entfernen leer ist, entfernen wir auch den Schlüssel aus der Map.
                    if vec_requests.is_empty() {
                        ready_state.req_tree.remove(&key);
                    }
                }
            }
        }

        debug!("Thread entfernt mit ID {}", target_id as i32);
        //Hat Einfügen in den Baum geklappt?
        let l = ready_state.req_tree.len();
        for r in &ready_state.req_tree {
            for v in r.1 {
                debug!("Request mit vd {} vorhanden", v.vd);
            }
        }
        debug!("Tree Size: {}", l as i32);

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

            debug!("Kill Thread {}", current.id());

        }

        let state = self.get_ready_state_and_join_map();
        let mut ready_state = state.0;
        let mut join_map = state.1;

        let join_list = join_map.get_mut(&thread_id).expect("Missing join map entry!");

        for thread in join_list {
            ready_state.ready_queue.push_front(Rc::clone(thread));
        }

        join_map.remove(&thread_id);
        ready_state.ready_queue.retain(|thread| thread.id() != thread_id);
    }

    /// 
    /// Description: Block calling thread
    /// 
    /// Parameters: `state` ReadyState of scheduler 
    /// MS -> why this param?
    /// 
    fn block(&self, state: &mut ReadyState) {
        let mut next_thread = state.ready_queue.pop_back();

        {
            // Execute in own block, so that the lock is released automatically (block() does not return)
            let mut sleep_list = self.sleep_list.lock();
            while next_thread.is_none() {
                Scheduler::check_sleep_list(state, &mut sleep_list);
                next_thread = state.ready_queue.pop_back();
            }
        }

        let current = Scheduler::current(&state);
        let next = next_thread.unwrap();

        // Thread has enqueued itself into sleep list and waited so long, that it dequeued itself in the meantime
        if current.id() == next.id() {
            return;
        }

        let current_ptr = ptr::from_ref(current.as_ref());
        let next_ptr = ptr::from_ref(next.as_ref());

        state.current_thread = Some(next);
        drop(current); // Decrease Rc manually, because Thread::switch does not return

        unsafe {
            Thread::switch(current_ptr, next_ptr);
        }
    }

    /// Description: Return current running thread
    fn current(state: &ReadyState) -> Rc<Thread> {
        Rc::clone(state.current_thread.as_ref().expect("Trying to access current thread before initialization!"))
    }

    fn check_sleep_list(state: &mut ReadyState, sleep_list: &mut Vec<(Rc<Thread>, usize)>) {
        let time = timer().systime_ms();

        //falls die sleep-Zeit abgelaufen ist, wird es in die ready queue gepusht, sonst bleibt es in der Liste
        sleep_list.retain(|entry| {
            if time >= entry.1 {
                //debug!("Thread ist wach ab {}", timer().systime_ms() as i32);
                state.ready_queue.push_front(Rc::clone(&entry.0));

                return false;
            }
            return true;
        });
    }

    fn check_sleep_list_eevdf(state: &mut ReadyState) {
        let time = timer().systime_ms();
        let weight = &mut state.weight;

        state.sleep_list_eevdf.retain(|entry| {
            if time >= entry.1 {
                //debug!("Thread ist wach ab {}", timer().systime_ms() as i32);

                //EEVDF
                *weight += 1;
                //Werte des Requests in der sleep-Liste anpassen
                //angepassten Request wieder in Baum einfügen
                //vt muss durch Lag angepasst werden

                //Key bereits vorhanden
                if let Some(vec_requests) = state.req_tree.get_mut(&entry.0.vd) {
                    vec_requests.push(entry.0.clone());
                } 
                else {
                    //neu hinzufügen
                    let key = entry.0.vd;
                    let mut vec_req  = Vec::new();
                    vec_req.push(entry.0.clone());
                    state.req_tree.insert(key, vec_req);   
                }
                return false;
            }
            return true;
        });
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
    fn get_ready_state_and_join_map(&self) -> (MutexGuard<ReadyState>, MutexGuard<Map<usize, Vec<Rc<Thread>>>>) {
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

    /// Aktualisiert die Accounting-Daten des aktuell laufenden Threads.
    pub fn update_current_thread_accounting(&self) {
        if let Some(mut state) = self.ready_state.try_lock() {
            if !state.initialized {
                return;
            }
            //FRAGE: Wieso geht es wenn ich try.lock() mache bzw was macht das???

        //let state = self.get_ready_state();
        let current = Scheduler::current(&state);

        self.update_lag(&mut state);
    
    
        let current_time = timer().systime_ms();
        //debug!("Updating thread accounting. Current time: {}", current_time);

        current.update_accounting(current_time as i32);
        //debug!("Updated thread runtime: {}", current.get_accounting());
        
        }

    }

    /// Prüft, ob der aktuell laufende Thread bereits sein Zeitquant überschritten hat.
    pub fn should_switch_thread(&self) -> bool {
        if let Some(mut state) = self.ready_state.try_lock() {
            if !state.initialized {
                return false;
            }
        let current = Scheduler::current(&state);

        debug!("Runtime: {} for Thread {}", current.get_accounting(), current.id());

        // Beispiel: Wechsel erzwingen, wenn der Thread länger als 10ms lief
        return current.get_accounting() >= 10;
        }
        return false;
    }

    pub fn reset_accounting(&self) {
        if let Some(mut state) = self.ready_state.try_lock() {
            if !state.initialized {
                return;
            }
        let current = Scheduler::current(&state);
        
        // Beispiel: Wechsel erzwingen, wenn der Thread länger als 10ms lief
        debug!("RESET");

        current.reset_acc();
        }
        debug!("reset else");

    }

    fn update_lag(&self, state: &mut ReadyState) {
        let current = Scheduler::current(&state);

        state.virtual_time += current.get_accounting();


        if let Some(mut request) = state.find_request_for_thread(&current) {
            request.lag = 10 - current.get_accounting();
            debug!("Lag = {} mit ID {}, VT = {}", request.lag, current.id(), state.virtual_time);
        }
    }
}
