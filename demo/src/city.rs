//! Static city geometry and route planning.
//!
//! The city is a fixed `GRID_DISTRICTS x GRID_DISTRICTS` layout of districts, each a
//! `BLOCK x BLOCK` square of intersections. Districts can be activated and removed at runtime,
//! so routing always asks "does this node currently exist?" instead of assuming the full grid.

use std::collections::{HashMap, VecDeque};

use rand::{Rng, seq::SliceRandom};

pub const BLOCK: i32 = 6;
pub const GRID_DISTRICTS: i32 = 3;
pub const GRID_NODES: i32 = BLOCK * GRID_DISTRICTS;
pub const DISTRICT_COUNT: usize = (GRID_DISTRICTS * GRID_DISTRICTS) as usize;
pub const DEFAULT_ACTIVE: [usize; 4] = [0, 1, 3, 4];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Node {
    pub x: i16,
    pub y: i16,
}

/// The axis a vehicle travels along. `Ns` is vertical travel, `Ew` is horizontal travel.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Axis {
    Ns,
    Ew,
}

impl Axis {
    pub const fn index(self) -> usize {
        match self {
            Self::Ns => 0,
            Self::Ew => 1,
        }
    }

    #[must_use]
    pub const fn other(self) -> Self {
        match self {
            Self::Ns => Self::Ew,
            Self::Ew => Self::Ns,
        }
    }
}

impl Node {
    #[allow(clippy::cast_possible_truncation)]
    pub const fn new(x: i32, y: i32) -> Self {
        Self {
            x: x as i16,
            y: y as i16,
        }
    }

    pub fn in_city(x: i32, y: i32) -> bool {
        (0..GRID_NODES).contains(&x) && (0..GRID_NODES).contains(&y)
    }

    #[allow(clippy::cast_sign_loss)]
    pub fn district(self) -> usize {
        let (x, y) = (i32::from(self.x) / BLOCK, i32::from(self.y) / BLOCK);
        (y * GRID_DISTRICTS + x) as usize
    }

    pub fn junction_name(self) -> String {
        format!("i-{}-{}", self.x, self.y)
    }

    pub fn light_name(self) -> String {
        format!("l-{}-{}", self.x, self.y)
    }

    /// Parses `i-x-y` / `l-x-y` ids into the kind prefix and node.
    pub fn parse(id: &str) -> Option<(char, Self)> {
        let mut parts = id.split('-');
        let kind = parts.next()?.chars().next()?;
        let x: i32 = parts.next()?.parse().ok()?;
        let y: i32 = parts.next()?.parse().ok()?;
        (parts.next().is_none() && Self::in_city(x, y)).then(|| (kind, Self::new(x, y)))
    }

    /// Axis of travel when moving from `self` to the adjacent node `to`.
    pub fn axis_to(self, to: Self) -> Axis {
        if self.x == to.x { Axis::Ns } else { Axis::Ew }
    }

    pub fn neighbors(self) -> impl Iterator<Item = Self> {
        let (x, y) = (i32::from(self.x), i32::from(self.y));
        [(x + 1, y), (x - 1, y), (x, y + 1), (x, y - 1)]
            .into_iter()
            .filter(|&(x, y)| Self::in_city(x, y))
            .map(|(x, y)| Self::new(x, y))
    }
}

#[allow(clippy::cast_possible_wrap)]
pub fn district_nodes(district: usize) -> impl Iterator<Item = Node> {
    let dx = (district as i32 % GRID_DISTRICTS) * BLOCK;
    let dy = (district as i32 / GRID_DISTRICTS) * BLOCK;
    (0..BLOCK).flat_map(move |y| (0..BLOCK).map(move |x| Node::new(dx + x, dy + y)))
}

#[allow(clippy::cast_possible_wrap)]
pub fn district_center(district: usize) -> Node {
    let dx = (district as i32 % GRID_DISTRICTS) * BLOCK;
    let dy = (district as i32 / GRID_DISTRICTS) * BLOCK;
    Node::new(dx + BLOCK / 2, dy + BLOCK / 2)
}

pub fn district_name(district: usize) -> &'static str {
    const NAMES: [&str; DISTRICT_COUNT] = [
        "Harbor",
        "Old Town",
        "Uptown",
        "Market",
        "Downtown",
        "Campus",
        "Riverside",
        "Tech Park",
        "Airport",
    ];
    NAMES[district]
}

/// Breadth-first route from `from` to `to` over existing, unblocked nodes.
///
/// Neighbour order is shuffled so equal-length routes are spread across the grid instead of all
/// vehicles taking the same L-shaped path. The returned route excludes `from`.
pub fn plan_route(
    from: Node,
    to: Node,
    passable: impl Fn(Node) -> bool,
    rng: &mut impl Rng,
) -> Option<VecDeque<Node>> {
    if from == to {
        return Some(VecDeque::new());
    }
    let mut parent: HashMap<Node, Node> = HashMap::new();
    let mut queue = VecDeque::from([from]);
    while let Some(node) = queue.pop_front() {
        let mut next: Vec<Node> = node.neighbors().collect();
        next.shuffle(rng);
        for n in next {
            if n == from || parent.contains_key(&n) || !passable(n) {
                continue;
            }
            parent.insert(n, node);
            if n == to {
                let mut route = VecDeque::new();
                let mut cur = to;
                while cur != from {
                    route.push_front(cur);
                    cur = parent[&cur];
                }
                return Some(route);
            }
            queue.push_back(n);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_ids_round_trip() {
        let node = Node::new(7, 13);
        assert_eq!(Node::parse(&node.junction_name()), Some(('i', node)));
        assert_eq!(Node::parse(&node.light_name()), Some(('l', node)));
        assert_eq!(Node::parse("i-99-1"), None);
        assert_eq!(Node::parse("v-12"), None);
    }

    #[test]
    fn districts_partition_the_grid() {
        let mut seen = std::collections::HashSet::new();
        for d in 0..DISTRICT_COUNT {
            for node in district_nodes(d) {
                assert_eq!(node.district(), d);
                assert!(seen.insert(node));
            }
        }
        assert_eq!(seen.len(), (GRID_NODES * GRID_NODES) as usize);
    }

    #[test]
    fn routes_are_shortest_and_avoid_blocked_nodes() {
        let mut rng = rand::rng();
        let (a, b) = (Node::new(0, 0), Node::new(3, 2));
        let route = plan_route(a, b, |_| true, &mut rng).unwrap();
        assert_eq!(route.len(), 5);
        assert_eq!(route.back(), Some(&b));

        let wall = |n: Node| n.x != 1;
        assert!(plan_route(a, b, wall, &mut rng).is_none());

        let hole = |n: Node| n != Node::new(1, 0);
        let route = plan_route(a, Node::new(2, 0), hole, &mut rng).unwrap();
        assert!(!route.contains(&Node::new(1, 0)));
    }
}
