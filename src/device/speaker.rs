use spin::Mutex;
use x86_64::instructions::port::{Port, PortWriteOnly};
use crate::device::pit;

static SPEAKER: Mutex<Speaker> = Mutex::new(Speaker::new());

pub struct Speaker {
    ctrl_port: PortWriteOnly<u8>,
    data_port_2: PortWriteOnly<u8>,
    ppi_port: Port<u8>
}

pub fn get_speaker() -> &'static Mutex<Speaker> {
    return &SPEAKER;
}

impl Speaker {
    const fn new() -> Self {
        Self { ctrl_port: PortWriteOnly::new(0x43), data_port_2: PortWriteOnly::new(0x42), ppi_port: Port::new(0x61) }
    }

    pub fn on(&mut self, freq: usize) {
        let counter = pit::BASE_FREQUENCY / freq;

        unsafe {
            // Config counter
            self.ctrl_port.write(0xb6);
            self.data_port_2.write((counter % 256) as u8);
            self.data_port_2.write((counter / 256) as u8);

            // Turn speaker on
            let status = self.ppi_port.read();
            self.ppi_port.write(status | 0x03);
        }
    }

    pub fn off(&mut self) {
        unsafe {
            let status = self.ppi_port.read();
            self.ppi_port.write(status & 0xfc);
        }
    }

    pub fn play(&mut self, freq: usize, duration_ms: usize) {
        self.on(freq);
        pit::wait(duration_ms);
        self.off();
    }
}