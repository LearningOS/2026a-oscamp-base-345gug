//! # Channel Communication
//!
//! In this exercise, you will use `std::sync::mpsc` channels to pass messages between threads.
//!
//! ## Concepts
//! - `mpsc::channel()` creates a multiple producer, single consumer channel
//! - `Sender::send()` sends a message
//! - `Receiver::recv()` receives a message
//! - Multiple producers can be created via `Sender::clone()`

use std::sync::mpsc;
use std::thread;

/// Create a producer thread that sends each element from items into the channel.
/// The main thread receives all messages and returns them.
pub fn simple_send_recv(items: Vec<String>) -> Vec<String> {
    // TODO: Create channel
    // TODO: Spawn thread to send each element in items
    // TODO: In main thread, receive all messages and collect into Vec
    // Hint: When all Senders are dropped, recv() returns Err
    let (tx,rx)=mpsc::channel();
    let mut vec = Vec::new();
    let tx_clone = tx.clone();
    let h = thread::spawn(move || {
        // items 在这里一次性 move 进子线程，遍历发送
        for s in items {
            tx_clone.send(s).expect("send failed");
        }
    });   
    drop(tx);
    loop {
        match rx.recv() {
            Ok(val) => vec.push(val),
            Err(_) => break, // 所有sender全部销毁，通道关闭
        }
    }   
    h.join().unwrap();
    vec
    

}

/// Create `n_producers` producer threads, each sending a message in format `"msg from {id}"`.
/// Collect all messages, sort them lexicographically, and return.
///
/// Hint: Use `tx.clone()` to create multiple senders. Note that the original tx must also be dropped.
pub fn multi_producer(n_producers: usize) -> Vec<String> {
    // TODO: Create channel
    // TODO: Clone a sender for each producer
    // TODO: Remember to drop the original sender, otherwise receiver won't finish
    // TODO: Collect all messages and sort
    let (tx,rx) = mpsc::channel();
    let mut handles = Vec::new();
    let mut res_vec = Vec::new();
    for id in 0..n_producers{
        let tx_clone=tx.clone();
        let h = thread::spawn(move || {
            let msg = format!("msg from {}",id);
            tx_clone.send(msg).expect("failure");
        });
        handles.push(h);
    }
    drop(tx);
    for h in handles{
        h.join().unwrap();
    }
    for _ in 0..n_producers {
        let s = rx.recv().unwrap();
        res_vec.push(s);
    }    
    res_vec.sort();
    res_vec
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_send_recv() {
        let items = vec!["hello".into(), "world".into(), "rust".into()];
        let result = simple_send_recv(items.clone());
        assert_eq!(result, items);
    }

    #[test]
    fn test_simple_empty() {
        let result = simple_send_recv(vec![]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_multi_producer() {
        let result = multi_producer(3);
        assert_eq!(
            result,
            vec![
                "msg from 0".to_string(),
                "msg from 1".to_string(),
                "msg from 2".to_string(),
            ]
        );
    }

    #[test]
    fn test_multi_producer_single() {
        let result = multi_producer(1);
        assert_eq!(result, vec!["msg from 0".to_string()]);
    }
}
