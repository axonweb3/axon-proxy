use std::hash::{Hash, Hasher};

pub fn rendezvous_hashing<N: Hash>(
    nodes: impl IntoIterator<Item = N>,
    key: impl Hash,
    hasher: impl Hasher + Clone,
) -> Option<usize> {
    nodes
        .into_iter()
        .enumerate()
        .map(|(idx, n)| {
            let mut s = hasher.clone();
            key.hash(&mut s);
            n.hash(&mut s);
            let h = s.finish();
            (h, idx)
        })
        .max_by_key(|(h, _)| *h)
        .map(|(_, idx)| idx)
}

pub trait NodeWithWeight {
    type T: Hash;
    fn tag(&self) -> Self::T;
    /// Weight must positive and normal.
    fn weight(&self) -> f64;
}

impl<'a, T: Hash> NodeWithWeight for &'a (T, f64) {
    type T = &'a T;
    fn tag(&self) -> Self::T {
        &self.0
    }
    fn weight(&self) -> f64 {
        self.1
    }
}

pub fn weighted_rendezvous_hashing<N: NodeWithWeight>(
    nodes: impl IntoIterator<Item = N>,
    key: impl Hash,
    hasher: impl Hasher + Clone,
) -> Option<usize> {
    nodes
        .into_iter()
        .enumerate()
        .map(|(idx, n)| {
            let mut s = hasher.clone();
            key.hash(&mut s);
            n.tag().hash(&mut s);
            let hash = s.finish();
            // https://www.snia.org/sites/default/files/SDC15_presentations/dist_sys/Jason_Resch_New_Consistent_Hashings_Rev.pdf
            //
            // This is 0 when hash is 0, -inf when hash is u64::MAX.
            (-n.weight() / (hash as f64 / u64::MAX as f64).ln(), idx)
        })
        .max_by(|(w1, _), (w2, _)| w1.partial_cmp(w2).unwrap_or(std::cmp::Ordering::Less))
        .map(|(_, idx)| idx)
}

pub struct RendezvousHashing<H, N> {
    pub nodes: Vec<N>,
    state: H,
}

impl<H, N> RendezvousHashing<H, N>
where
    H: Hasher + Clone,
    N: NodeWithWeight,
{
    /// Choose a node based on key.
    ///
    /// Returns None only if there are no nodes.
    pub fn choose<K>(&self, key: K) -> Option<usize>
    where
        K: Hash,
    {
        self.nodes
            .iter()
            .enumerate()
            .map(|(idx, n)| {
                let mut s = self.state.clone();
                key.hash(&mut s);
                n.tag().hash(&mut s);
                let hash = s.finish();
                // https://www.snia.org/sites/default/files/SDC15_presentations/dist_sys/Jason_Resch_New_Consistent_Hashings_Rev.pdf
                //
                // This is 0 when hash is 0, -inf when hash is u64::MAX.
                (-n.weight() / (hash as f64 / u64::MAX as f64).ln(), idx)
            })
            .max_by(|(w1, _), (w2, _)| w1.partial_cmp(w2).unwrap_or(std::cmp::Ordering::Less))
            .map(|wn| wn.1)
    }
}

#[cfg(test)]
mod tests {
    use siphasher::sip::SipHasher;

    use super::*;

    #[test]
    fn test_distribution() {
        let nodes = [0, 1, 2, 3, 4];

        let mut choosen = [0, 0, 0, 0, 0];

        for i in 0..10000 {
            choosen[rendezvous_hashing(nodes, i, SipHasher::new()).unwrap()] += 1;
        }

        for c in choosen {
            println!("{}", c as f64 / 10000.);
        }
    }

    #[test]
    fn test_distribution_weighted() {
        let nodes = [(0, 4.), (1, 3.), (2, 2.), (3, 1.)];

        let mut choosen = [0, 0, 0, 0];

        for i in 0..10000 {
            choosen[weighted_rendezvous_hashing(&nodes, i, SipHasher::new()).unwrap()] += 1;
        }

        for c in choosen {
            println!("{}", c as f64 / 10000.);
        }
    }
}
