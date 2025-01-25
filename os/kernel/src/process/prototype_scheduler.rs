extern crate alloc;
use alloc::collections::BTreeMap;
use core::sync::atomic::AtomicUsize;
use core::sync::atomic::Ordering::Relaxed;
use alloc::vec::Vec;
use alloc::vec;
use alloc::string::String;
use crate::alloc::string::ToString;

// Thread Dummy (später Thread Klasse)

// thread IDs
static THREAD_ID_COUNTER: AtomicUsize = AtomicUsize::new(1); //der erste Thread hat die ID = 1 
static REQUEST_ID_COUNTER: AtomicUsize = AtomicUsize::new(1); //der erste Thread hat die ID = 1 

pub fn next_thread_id() -> i32 {
    THREAD_ID_COUNTER.fetch_add(1, Relaxed) as i32 //fetch_add = ID um 1 erhöhen und returnen
}

//irgendwann auf 0 zurücksetzen?
pub fn next_request_id() -> i32 {
    REQUEST_ID_COUNTER.fetch_add(1, Relaxed) as i32 //fetch_add = ID um 1 erhöhen und returnen
}

#[derive(Debug, Clone)]
struct Thread {
    id: i32,
    status: String,
    duration: i32,
    request: Request,
}

// jeder Thread soll 10ms rechnen
const CALC_TIME: i32 = 10;

#[derive(Debug, Clone, PartialEq)]
struct Request {
    virtual_deadline: i32,
    virtual_eligible_time: i32,
    lag: i32,
    id: i32
}

#[derive(Debug)]
struct Global {
    virtual_time: i32,
    total_weight: i32,
    request_tree: BTreeMap<(i32, i32, i32), Request>, // Keyed by virtual_deadline
}

#[derive(Debug)]
struct Scheduler {
    global: Global,
}

impl Scheduler {
    fn new() -> Self {
        Scheduler {
            global: Global {
                virtual_time: 100,
                total_weight: 0,
                request_tree: BTreeMap::new(),
            },
        }
    }

    fn join(&mut self, thread: Thread) {
        println!(
            "Thread {} is joining with request {:?} and is {}",
            thread.id, thread.request, thread.status
        );
        self.global
            .total_weight += 1;

        self.global
            .virtual_time = self.global.virtual_time - thread.request.lag / self.global.total_weight;

        println!(
            "Total weight is now {} and the Virtual Time is {}",
            self.global.total_weight, self.global.virtual_time
        );

        self.global
            .request_tree
            .insert((thread.request.virtual_deadline, thread.request.virtual_eligible_time, thread.request.id), thread.request.clone());
    }

    fn next_request(&mut self, threads: &mut Vec<Thread>)  {

        // Wähle den Request mit der niedrigsten virtual_deadline aus
        let req = self.find_request().unwrap();
        let req2 = req.clone(); // Kopiere den Request für die Rückgabe

        let key = (req2.virtual_deadline, req2.virtual_eligible_time, req2.id); // Speichere den Schlüssel
              
        println!("Selected request with virtual_deadline {}", key.0);

        // Finde den zugehörigen Thread und simuliere Rechenzeit
        if self.calc_req(threads, &req2) {
            self.global.request_tree.remove(&key);
            self.global.request_tree.insert(key, req2);
        }
    }

    fn calc_req(&mut self, threads: &mut Vec<Thread>, req: &Request) -> bool {
        // Finde den zugehörigen Thread und simuliere Rechenzeit
        let mut req2 = req.clone();

        for thread in threads.iter_mut() {
            if thread.request == req2 {
                println!("Request aus Thread {:?} entspricht Request aus Baum {:?}",
                thread.request, req2);

                println!(
                    "Updating thread {}: Reducing duration from {} to {}",
                    thread.id,
                    thread.duration,
                    thread.duration - CALC_TIME
                );

                //Rechnet für 10ms
                thread.duration -= CALC_TIME; //sollte eigentlich Rechenzeit zurückgeben

                //Hier festlegen wie lang gerechnet wurde um Lag zu simulieren
                let actual_calc_time = 10;

                req2.lag = actual_calc_time - CALC_TIME;
                thread.request.lag = actual_calc_time - CALC_TIME;

                self.global.virtual_time += actual_calc_time;

                //Aufgabe des Threads erfüllt?
                if thread.duration <= 0 {
                    self.global.request_tree.remove(&(req2.virtual_deadline, req2.virtual_eligible_time, req2.id));
                    self.leave(thread);
                    return false;
                }

                break;
            }
        }
        return true;
    }


    fn find_request(&mut self) -> Option<&Request> {
        for t in &self.global.request_tree {
            if t.0.1 <= self.global.virtual_time {
                return Some(t.1);
            }
        }
        return None; 
    }

    fn leave(&mut self, thread: &mut Thread) {
        println!(
            "Thread {} is leaving and request {:?} is fulfilled",
            thread.id, thread.request
        );
        self.global
            .total_weight -= 1;

        if self.global.total_weight > 0 {
            self.global
            .virtual_time = self.global.virtual_time + thread.request.lag / self.global.total_weight;
        }

        println!(
            "Total weight is now {} and the Virtual Time is {}",
            self.global.total_weight, self.global.virtual_time
        );
    }
}


fn main() {
    let mut scheduler = Scheduler::new();

    // Beispiel-Threads erstellen
    let mut threads = vec![
        Thread {
            id: next_thread_id(),
            status: "ready".to_string(),
            duration: 50,
            request: Request {
                virtual_deadline: 100,
                virtual_eligible_time: 10,
                lag: 0,
                id: next_request_id()
            },
        },
        Thread {
            id: next_thread_id(),
            status: "ready".to_string(),
            duration: 30,
            request: Request {
                virtual_deadline: 80,
                virtual_eligible_time: 5,
                lag: 0,
                id: next_request_id()
            },
        },
        Thread {
            id: next_thread_id(),
            status: "ready".to_string(),
            duration: 20,
            request: Request {
                virtual_deadline: 120,
                virtual_eligible_time: 60,
                lag: 0,
                id: next_request_id()
            },
        },
    ];

    // Threads dem Scheduler hinzufügen
    for thread in &threads {
        scheduler.join(thread.clone());
    }

    // Nächsten Request auswählen und zugehörigen Thread bearbeiten
    scheduler.next_request(&mut threads);
    scheduler.next_request(&mut threads);
    scheduler.next_request(&mut threads);
    scheduler.next_request(&mut threads);
    scheduler.next_request(&mut threads);
    scheduler.next_request(&mut threads);
    scheduler.next_request(&mut threads);
    scheduler.next_request(&mut threads);
    scheduler.next_request(&mut threads);
    scheduler.next_request(&mut threads);
}