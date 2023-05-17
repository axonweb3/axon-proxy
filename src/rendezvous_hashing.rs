use std::{
    borrow::Borrow,
    hash::{Hash, Hasher},
};

pub struct WeightedRendezvousHashing<H, N> {
    // Vec of (node, weight).
    nodes: Vec<(N, f64)>,
    state: H,
}

impl<H, N> WeightedRendezvousHashing<H, N>
where
    H: Hasher + Clone,
    N: Hash,
{
    /// Choose a node based on key.
    ///
    /// Returns None only if there are no nodes.
    pub fn choose<K>(&self, key: K) -> Option<&N>
    where
        K: Hash,
    {
        self.nodes
            .iter()
            .map(|(node, weight)| {
                let mut s = self.state.clone();
                key.hash(&mut s);
                node.hash(&mut s);
                let hash = s.finish();
                // https://www.snia.org/sites/default/files/SDC15_presentations/dist_sys/Jason_Resch_New_Consistent_Hashings_Rev.pdf
                //
                // This is 0 when hash is 0, -inf when hash is u64::MAX.
                (-weight / (hash as f64 / u64::MAX as f64).ln(), node)
            })
            .max_by(|(w1, _), (w2, _)| w1.partial_cmp(w2).unwrap_or(std::cmp::Ordering::Less))
            .map(|wn| wn.1)
    }

    pub fn new(state: H) -> Self {
        Self {
            nodes: Vec::new(),
            state,
        }
    }

    /// Add a node.
    ///
    /// You shouldn't add duplicated nodes.
    ///
    /// # Panics
    ///
    /// If the weight is not normal and positive.
    pub fn add(&mut self, node: N, weight: f64) {
        assert!(weight.is_normal() && weight > 0.);
        self.nodes.push((node, weight))
    }

    /// Remove nodes equal to node.
    pub fn remove<Q>(&mut self, node: &Q)
    where
        N: Borrow<Q>,
        Q: Eq,
    {
        self.nodes.retain(|(n, _)| n.borrow() != node);
    }
}

pub struct RendezvousHashing<H, N> {
    nodes: Vec<N>,
    state: H,
}

impl<H, N> RendezvousHashing<H, N>
where
    H: Hasher + Clone,
    N: Hash,
{
    /// Choose a node based on key.
    ///
    /// Returns None only if there are no nodes.
    pub fn choose<K>(&self, key: K) -> Option<&N>
    where
        K: Hash,
    {
        self.nodes.iter().max_by_key(|node| {
            let mut s = self.state.clone();
            key.hash(&mut s);
            node.hash(&mut s);
            s.finish()
        })
    }

    pub fn new(state: H) -> Self {
        Self {
            nodes: Vec::new(),
            state,
        }
    }

    /// Add a node.
    ///
    /// You shouldn't add duplicated nodes.
    pub fn add(&mut self, node: N) {
        self.nodes.push(node)
    }

    /// Remove nodes equal to node.
    pub fn remove<Q>(&mut self, node: &Q)
    where
        N: Borrow<Q>,
        Q: Eq,
    {
        self.nodes.retain(|n| n.borrow() != node);
    }
}

#[cfg(test)]
mod tests {
    use siphasher::sip::SipHasher;

    use super::*;

    #[test]
    fn test_distribution() {
        let mut rh = RendezvousHashing::new(SipHasher::new());

        rh.add(0);
        rh.add(1);
        rh.add(2);
        rh.add(3);
        rh.add(4);

        let mut choosen = [0, 0, 0, 0, 0];

        for i in 0..10000 {
            choosen[*rh.choose(i).unwrap() as usize] += 1;
        }

        for c in choosen {
            println!("{}", c as f64 / 10000.);
        }
    }

    #[test]
    fn test_distribution_weighted() {
        let mut wrh = WeightedRendezvousHashing::new(SipHasher::new());

        wrh.add(0, 4.);
        wrh.add(1, 3.);
        wrh.add(2, 2.);
        wrh.add(3, 1.);

        let mut choosen = [0, 0, 0, 0];

        for i in 0..10000 {
            choosen[*wrh.choose(i).unwrap() as usize] += 1;
        }

        for c in choosen {
            println!("{}", c as f64 / 10000.);
        }
    }
}
