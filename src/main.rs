mod threadpool;

fn main() {
    let threadpool = threadpool::Threadpool::new(5);

    threadpool.execute(|| {
        println!("Hello world");
    });
}
