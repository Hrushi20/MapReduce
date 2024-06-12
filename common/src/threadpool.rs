use std::sync::mpsc::Receiver;
use std::sync::mpsc::Sender;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;

type Job = Box<dyn FnOnce() + Send + 'static>;
pub struct Threadpool {
    // List of workers.
    threads: Vec<Worker>,
    sender: Option<Sender<Job>>, // Need to send func
}

impl Threadpool {
    pub fn new(size: usize) -> Self {
        assert!(size > 0);

        let (sender, rec) = mpsc::channel();
        let mut threads = Vec::with_capacity(size);
        let rec = Arc::new(Mutex::new(rec));

        for i in 0..size {
            threads.push(Worker::new(Arc::clone(&rec), i));
        }

        Threadpool {
            threads,
            sender: Some(sender),
        }
    }

    pub fn execute<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);
        self.sender.as_ref().unwrap().send(job).unwrap();
    }
}

impl Drop for Threadpool {
    fn drop(&mut self) {
        drop(self.sender.take());
        for worker in &mut self.threads {
            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

// Perform Work assigned to it.
struct Worker {
    thread: Option<JoinHandle<()>>,
    id: usize,
}

impl Worker {
    fn new(rec: Arc<Mutex<Receiver<Job>>>, id: usize) -> Self {
        // Create a Channel a listen to it.
        let thread = thread::spawn(move || loop {
            let message = rec.lock().unwrap().recv();

            match message {
                Ok(job) => {
                    job();
                }
                Err(_) => {
                    break;
                }
            }
        });

        Worker {
            thread: Some(thread),
            id,
        }
    }
}
