// Copyright 2025 The kmesh Authors
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

use std::{fmt::Debug, sync::Arc};

use super::{Balancer, WeightedEndpoint};

/// The round robin load balancer select 1 available host in round robin order using
/// the smooth weighted round-robin balancing algorithm.
///
/// See implementation in nginx
/// <https://github.com/phusion/nginx/commit/27e94984486058d73157038f7950a0a36ecc6e35/>.

#[derive(Clone, Debug)]
pub struct LbItem<E> {
    weight: u32,
    current_weight: i32,
    item: Arc<E>,
}

impl<E> LbItem<E> {
    pub fn new(weight: u32, item: Arc<E>) -> Self {
        Self { item, weight, current_weight: 0 }
    }

    #[inline]
    fn increase_current_weight(&mut self) {
        self.adjust_current_weight(i32::try_from(self.weight).unwrap_or(i32::MAX));
    }

    #[inline]
    fn adjust_current_weight(&mut self, value: i32) {
        self.current_weight = self.current_weight.saturating_add(value);
    }
}

#[derive(Debug, Clone)]
pub struct WeightedRoundRobinBalancer<E> {
    items: Vec<LbItem<E>>,
    total_weight: i32,
    equal_weight: bool,
    current_index: usize,
}

impl<E> WeightedRoundRobinBalancer<E> {
    pub fn new(items: impl IntoIterator<Item = LbItem<E>>) -> Self {
        let (items, total_weight, equal_weight) = collect_checked(items);
        WeightedRoundRobinBalancer {
            items,
            total_weight: i32::try_from(total_weight).unwrap_or(i32::MAX),
            equal_weight,
            current_index: 0,
        }
    }
}

/// Returns a valid subset of the items whose total weight fits in [u32].
fn collect_checked<E>(items: impl IntoIterator<Item = LbItem<E>>) -> (Vec<LbItem<E>>, u32, bool) {
    let mut total = 0_u32;
    let mut first_weight = None;
    let mut equal_weight = true;
    (items
        .into_iter()
        .take_while(|item| {
            if let Some(first_weight) = first_weight {
                if equal_weight && first_weight != item.weight {
                    equal_weight = false;
                }
            } else {
               first_weight = Some(item.weight);
            }

            let result = total.checked_add(item.weight);
            if let Some(new_total) = result {
                total = new_total;
            } else {
                tracing::warn!("Endpoint weight overflow in round robin load balancer, will only use the endpoints whose weight sum fits in 32 bits");
            }
            result.is_some()
        })
        .collect(), total, equal_weight)
}

impl<E: WeightedEndpoint> Default for WeightedRoundRobinBalancer<E> {
    fn default() -> Self {
        Self::from_iter([])
    }
}

impl<T> Balancer<T> for WeightedRoundRobinBalancer<T> {
    fn next_item(&mut self, _hash: Option<u64>) -> Option<&T> {
        if self.equal_weight {
            // Simple round robin if all weights are equal
            if self.items.len() <= 1 {
                return self.items.first().map(|item| item.item.as_ref());
            }
            let index = self.current_index;
            self.current_index = (self.current_index + 1) % self.items.len();
            return self.items.get(index).map(|item| item.item.as_ref());
        }

        if self.items.len() <= 1 {
            return self.items.first().map(|item| item.item.as_ref());
        }

        // Increase the current weight of all the endpoints and pick the first max.
        // Note: not using `max_by` here because it returns the last element for equal weights.
        //
        let mut best_index = 0;
        let mut best_weight = i32::MIN;
        for (idx, item) in self.items.iter_mut().enumerate() {
            item.increase_current_weight();
            if item.current_weight > best_weight {
                best_weight = item.current_weight;
                best_index = idx;
            }
        }
        let item = self.items.get_mut(best_index)?;
        item.adjust_current_weight(-self.total_weight);
        Some(item.item.as_ref())
    }
}

impl<E: WeightedEndpoint> FromIterator<Arc<E>> for WeightedRoundRobinBalancer<E> {
    fn from_iter<T: IntoIterator<Item = Arc<E>>>(iter: T) -> Self {
        Self::new(iter.into_iter().map(|item| LbItem::new(item.weight(), item)))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::clusters::balancers::Balancer;

    use super::{LbItem, WeightedRoundRobinBalancer};

    fn compare_rotated<T: PartialEq>(a: impl IntoIterator<Item = T>, b: impl IntoIterator<Item = T>) {
        let mut a = a.into_iter().peekable();
        let mut b = b.into_iter().peekable();
        while a.peek() != b.peek() {
            a.next();
        }

        assert!(a.zip(b).all(|(a, b)| a == b));
    }

    #[test]
    pub fn test_wrr_balancer_0() {
        #[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
        struct Blah {
            value: u32,
        }
        let b1 = Blah { value: 0 };
        let b2 = Blah { value: 1 };

        let ab1 = Arc::new(b1);
        let ab2 = Arc::new(b2);
        let items = [Arc::clone(&ab1), Arc::clone(&ab2)];

        let lb_items = [LbItem::new(1, ab1), LbItem::new(1, ab2)];

        let mut wrr = WeightedRoundRobinBalancer::new(lb_items);
        let mut selected_items = vec![];
        for _n in 0..7 {
            selected_items.push(wrr.next_item(None).cloned());
        }

        compare_rotated(selected_items, items.into_iter().map(|item| Some((*item).clone())));
    }

    #[test]
    pub fn test_wrr_balancer_1() {
        #[derive(Debug, Clone, PartialEq)]
        struct Blah {
            value: u32,
        }
        let b1 = Blah { value: 0 };
        let b2 = Blah { value: 1 };
        let b3 = Blah { value: 2 };

        let ab1 = Arc::new(b1);
        let ab2 = Arc::new(b2);
        let ab3 = Arc::new(b3);

        let items = [Arc::clone(&ab1), Arc::clone(&ab2), Arc::clone(&ab3)];

        let lb_items = [LbItem::new(1, ab1), LbItem::new(1, ab2), LbItem::new(1, ab3)];

        let mut wrr = WeightedRoundRobinBalancer::new(lb_items);
        let mut selected_items = vec![];
        for _n in 0..10 {
            selected_items.push(wrr.next_item(None).cloned());
        }

        println!("Selected: {selected_items:?}");

        compare_rotated(selected_items, items.into_iter().map(|item| Some((*item).clone())));
    }

    #[test]
    #[allow(clippy::indexing_slicing)]
    pub fn test_wrr_balancer_2() {
        let mut items = vec![];
        let mut counts = vec![];
        for n in 0..3 {
            items.push(LbItem::new(1, Arc::new(n)));
            counts.push(0);
        }
        let item = &mut items[2];
        item.weight = 2;

        let mut wrr = WeightedRoundRobinBalancer::new(items);
        let mut selected_items = vec![];
        for _n in 0..20 {
            selected_items.push(wrr.next_item(None).copied());
        }
        let mut selected_items: Vec<_> = selected_items.into_iter().flatten().collect();
        selected_items.sort_unstable();

        for i in selected_items {
            counts[i] += 1;
        }
        assert_eq!(counts, vec![5, 5, 10]);
    }

    #[test]
    #[allow(clippy::indexing_slicing)]
    pub fn test_wrr_balancer_3() {
        let mut items = vec![];
        let mut counts = vec![];
        for n in 0..3 {
            items.push(LbItem::new(1, Arc::new(n)));
            counts.push(0);
        }
        let item = &mut items[1];
        item.weight = 2;
        let item = &mut items[2];
        item.weight = 4;

        let mut wrr = WeightedRoundRobinBalancer::new(items);
        let mut selected_items = vec![];
        for _n in 0..20 {
            selected_items.push(wrr.next_item(None).copied());
        }

        for i in selected_items.into_iter().flatten() {
            counts[i] += 1;
        }

        assert_eq!(counts, vec![3, 6, 11]);
    }

    #[test]
    /// Just make sure that the balancer does not panic.
    fn test_wrr_balancer_weight_overflow() {
        let items = [
            LbItem::new(u32::MAX / 2, Arc::new(0)),
            LbItem::new(u32::MAX / 2, Arc::new(1)),
            LbItem::new(u32::MAX / 2, Arc::new(2)),
        ];
        let mut wrr = WeightedRoundRobinBalancer::new(items);
        for _ in 0..20 {
            // Not checking which items have been chosen, as the arithmetic conversions distort the algorithm too much.
            // Just make sure something is chosen.
            assert!(wrr.next_item(None).is_some(), "expected balancer to select one item");
        }
    }
}
