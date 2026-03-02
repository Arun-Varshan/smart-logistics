import math
import random
import time
from collections import deque

# =========================
# BIGGER DIGITAL TWIN SIZE
# =========================

CANVAS_WIDTH = 1400
CANVAS_HEIGHT = 700
CELL = 25   # Larger grid cell for smoother movement

GRID_W = CANVAS_WIDTH // CELL
GRID_H = CANVAS_HEIGHT // CELL

# =========================
# DOCK AREA (Bigger & Full Height)
# =========================

DOCK_AREA = {
    "x": 0,
    "y": 0,
    "w": 100,
    "h": CANVAS_HEIGHT
}

# =========================
# RESCALED ZONES (Warehouse Layout)
# =========================

ZONES = {
    "Medical": {
        "x": 150, "y": 100,
        "w": 300, "h": 200,
        "color": "#ff6b6b"
    },
    "Fragile": {
        "x": 550, "y": 100,
        "w": 300, "h": 200,
        "color": "#f5a623"
    },
    "Electronics": {
        "x": 950, "y": 100,
        "w": 300, "h": 200,
        "color": "#4de1c9"
    },
    "Perishable": {
        "x": 150, "y": 380,
        "w": 300, "h": 220,
        "color": "#33d17a"
    },
    "Heavy": {
        "x": 550, "y": 380,
        "w": 300, "h": 220,
        "color": "#5b8cff"
    },
    "General": {
        "x": 950, "y": 380,
        "w": 300, "h": 220,
        "color": "#a9b0c7"
    },
}

ZONE_CAPACITY = {z: 150 for z in ZONES.keys()}

class Grid:
    def __init__(self):
        self.w = GRID_W
        self.h = GRID_H
        self.blocked = [[False for _ in range(self.h)] for _ in range(self.w)]
        for z in ZONES.values():
            x1 = max(0, z["x"] // CELL)
            y1 = max(0, z["y"] // CELL)
            x2 = min(self.w - 1, (z["x"] + z["w"]) // CELL)
            y2 = min(self.h - 1, (z["y"] + z["h"]) // CELL)
            for x in range(x1, x2 + 1):
                for y in range(y1, y2 + 1):
                    self.blocked[x][y] = False
        dock_x1 = DOCK_AREA["x"] // CELL
        dock_y1 = DOCK_AREA["y"] // CELL
        dock_x2 = (DOCK_AREA["x"] + DOCK_AREA["w"]) // CELL
        dock_y2 = (DOCK_AREA["y"] + DOCK_AREA["h"]) // CELL
        for x in range(dock_x1, min(dock_x2 + 1, self.w)):
            for y in range(dock_y1, min(dock_y2 + 1, self.h)):
                self.blocked[x][y] = False

    def line_free(self, a_px, b_px): 
        """Bresenham / sampling line-of-sight check on grid.""" 
        ax, ay = a_px[0] // CELL, a_px[1] // CELL 
        bx, by = b_px[0] // CELL, b_px[1] // CELL 

        dx = abs(bx - ax) 
        dy = abs(by - ay) 
        x, y = ax, ay 
        sx = 1 if bx >= ax else -1 
        sy = 1 if by >= ay else -1 

        if dx >= dy: 
            err = dx / 2.0 
            while x != bx: 
                if self.blocked[x][y]: 
                    return False 
                err -= dy 
                if err < 0: 
                    y += sy 
                    err += dx 
                x += sx 
        else: 
            err = dy / 2.0 
            while y != by: 
                if self.blocked[x][y]: 
                    return False 
                err -= dx 
                if err < 0: 
                    x += sx 
                    err += dy 
                y += sy 

        return not self.blocked[bx][by] 

    def smooth_path(self, path): 
        """Reduce waypoints using line-of-sight (string pulling).""" 
        if not path or len(path) < 3: 
            return path 

        smoothed = [path[0]] 
        i = 0 
        while i < len(path) - 1: 
            j = len(path) - 1 
            # Find farthest reachable point 
            while j > i + 1: 
                if self.line_free(smoothed[-1], path[j]): 
                    break 
                j -= 1 
            smoothed.append(path[j]) 
            i = j 
        return smoothed

    def neighbors(self, x, y):
        # Allow diagonal movement for smoother paths
        dirs = [(1,0),(-1,0),(0,1),(0,-1), (1,1), (-1,-1), (1,-1), (-1,1)]
        for dx, dy in dirs:
            nx, ny = x + dx, y + dy
            if 0 <= nx < self.w and 0 <= ny < self.h and not self.blocked[nx][ny]:
                yield nx, ny

    def heuristic(self, a, b):
        # Euclidean for diagonal
        return math.sqrt((a[0] - b[0])**2 + (a[1] - b[1])**2)

    def astar(self, start_px, end_px):
        sx, sy = start_px[0] // CELL, start_px[1] // CELL
        ex, ey = end_px[0] // CELL, end_px[1] // CELL
        open_set = set()
        open_set.add((sx, sy))
        came = {}
        g = {(sx, sy): 0}
        f = {(sx, sy): self.heuristic((sx, sy), (ex, ey))}
        while open_set:
            current = min(open_set, key=lambda c: f.get(c, float("inf")))
            if current == (ex, ey):
                path = deque()
                while current in came:
                    cx, cy = current
                    path.appendleft((cx * CELL + CELL // 2, cy * CELL + CELL // 2))
                    current = came[current]
                path.appendleft((sx * CELL + CELL // 2, sy * CELL + CELL // 2))
                return self.smooth_path(list(path))
            open_set.remove(current)
            for n in self.neighbors(current[0], current[1]):
                tentative = g[current] + 1
                if tentative < g.get(n, float("inf")):
                    came[n] = current
                    g[n] = tentative
                    f[n] = tentative + self.heuristic(n, (ex, ey))
                    if n not in open_set:
                        open_set.add(n)
        return []

class Robot:
    def __init__(self, robot_id, grid, robot_type="Standard"):
        self.id = robot_id
        self.x = DOCK_AREA["x"] + 10
        self.y = DOCK_AREA["y"] + DOCK_AREA["h"] // 2
        self.vx = 0
        self.vy = 0
        self.status = "idle"
        self.target_zone = None
        self.current_parcel = None
        self.path = []
        self.battery = 100
        self.grid = grid
        self.wait_until = 0
        self.deliver_until = 0
        self.collision_avoidance_active = False
        self.low_battery_threshold = 20
        self.notified_low_battery = False
        
        # Robot Type Properties
        self.type = robot_type
        if self.type == "Fast":
            self.speed_multiplier = 2.5 # Increased from 1.5
            self.battery_drain_rate = 0.2
            self.color = "#ff6b6b" 
        elif self.type == "Heavy":
            self.speed_multiplier = 1.2 # Increased from 0.8
            self.battery_drain_rate = 0.1
            self.color = "#5b8cff" 
        else: # Standard
            self.speed_multiplier = 1.8 # Increased from 1.0
            self.battery_drain_rate = 0.12
            self.color = "#4de1c9" 

        self.max_speed = 300.0 * self.speed_multiplier   # Increased from 160.0
        self.max_accel = 800.0                            # Increased from 420.0
        self.arrive_radius = 20.0 
        self.slow_radius = 80.0 
        self._last_t = time.time()

    def compute_velocity_avoidance(self, other_robots, preferred_velocity, time_horizon=2.0, radius=40):
        """
        Computes an adjusted velocity to avoid collisions with other robots.
        Uses a simplified ORCA-inspired approach (velocity obstacles).
        """
        vx, vy = preferred_velocity
        self.collision_avoidance_active = False
        
        # Tuning parameters
        avoid_radius = radius * 1.5  # Increased from default
        safety_margin = 35     # Increased from 25
        
        for other in other_robots:
            if other.id == self.id:
                continue
            
            # Only avoid active robots or those close by
            if other.status == "idle" and other.battery > 10:
                # Treat idle robots as static obstacles if close
                pass
            elif other.status not in ("moving_to_zone", "returning", "delivering", "charging"):
                 continue
        
            dx = other.x - self.x
            dy = other.y - self.y
            dist_sq = dx*dx + dy*dy
            
            # Quick check to skip far robots
            if dist_sq > (avoid_radius * 2)**2:
                continue
                
            dist = math.sqrt(dist_sq)
            if dist < 0.1: dist = 0.1 # Prevent division by zero
        
            # Relative velocity (my velocity - other's velocity)
            # We assume other robot keeps its current velocity
            other_vx = getattr(other, 'vx', 0)
            other_vy = getattr(other, 'vy', 0)
            
            rel_vx = vx - other_vx
            rel_vy = vy - other_vy
            
            # Check if we are moving towards each other
            # Project relative velocity onto the direction vector
            # dot product of rel_v and position_diff
            dot_prod = rel_vx * dx + rel_vy * dy
            
            # If dot_prod > 0, we are converging (distance is decreasing if we consider relative frame)
            # Wait, standard relative velocity check:
            # If we project rel_v onto P_other - P_self (which is dx, dy)
            # If dot > 0, we are moving towards the other in the relative frame
            
            if dot_prod > 0:
                # Time to collision (simplified)
                # speed towards other = dot_prod / dist
                closing_speed = dot_prod / dist
                time_to_collision = (dist - safety_margin) / closing_speed
                
                if time_to_collision < time_horizon:
                    # Avoidance needed!
                    self.collision_avoidance_active = True
                    
                    # Calculate avoidance force
                    # We want to push velocity perpendicular to the collision vector
                    
                    # Normalized direction to other
                    nx = dx / dist
                    ny = dy / dist
                    
                    # Perpendicular vector (rotate 90 deg)
                    perp_x = -ny
                    perp_y = nx
                    
                    # Choose side that is closer to current velocity or right side
                    # determinant (2D cross product) to see which side we are on
                    det = rel_vx * ny - rel_vy * nx
                    
                    # If det > 0, we are to the "right" of the collision line? 
                    # Let's just push away from the collision course.
                    
                    # Strength increases as we get closer or time reduces
                    urgency = 1.0 - (time_to_collision / time_horizon)
                    if dist < safety_margin:
                        urgency = 1.0 # Max urgency if too close
                    
                    avoid_force = urgency * 2.0 # Reduced force for smoother gliding
                    
                    # Apply perpendicular push
                    side = 1 if det > 0 else -1
                    
                    # Adjust velocity - Gliding effect
                    vx += perp_x * side * avoid_force * 10 
                    vy += perp_y * side * avoid_force * 10
                    
                    # Also slow down slightly if very close, but don't stop
                    if dist < safety_margin * 1.2:
                        slow_factor = 0.6 + 0.4 * (dist / (safety_margin * 1.2)) 
                        vx *= slow_factor
                        vy *= slow_factor
                        
                        # Soft repulsion if TOO close
                        if dist < 12:
                            repel_strength = 1.5
                            vx += (self.x - other.x) * repel_strength
                            vy += (self.y - other.y) * repel_strength

        # Cap speed
        speed = math.sqrt(vx*vx + vy*vy)
        max_speed = 8.0 * self.speed_multiplier # Base speed approx 80 * 0.1
        if speed > max_speed:
            scale = max_speed / speed
            vx *= scale
            vy *= scale
            
        return vx, vy

    def assign_task(self, zone_name, parcel=None):
        self.target_zone = zone_name
        self.current_parcel = parcel
        z = ZONES[zone_name]
        tx = z["x"] + z["w"] // 2
        ty = z["y"] + z["h"] // 2
        self.path = self.grid.astar((int(self.x), int(self.y)), (tx, ty))
        self.status = "moving_to_zone"

    def move_step(self, now, other_robots):
        self.collision_avoidance_active = False
        
        if self.battery <= 0:
            self.status = "charging"
        if self.status == "charging":
            self.battery = min(100, self.battery + 0.5)
            if self.battery >= 100:
                self.status = "idle"
                self.path = []
            return
        if self.battery < 20 and self.status != "returning":
            dx = DOCK_AREA["x"] + DOCK_AREA["w"] // 2
            dy = DOCK_AREA["y"] + DOCK_AREA["h"] // 2
            self.path = self.grid.astar((int(self.x), int(self.y)), (dx, dy))
            self.status = "returning"
        if now < self.wait_until:
            return
        if self.status in ("moving_to_zone", "returning"):
            if not self.path:
                if self.status == "moving_to_zone":
                    self.status = "delivering"
                    self.deliver_until = now + 1.5
                elif self.status == "returning":
                    self.status = "charging"
                return
            
            nx, ny = self.path[0]
            dx = nx - self.x
            dy = ny - self.y
            dist = math.sqrt(dx * dx + dy * dy)
            
            if dist <= 5: # Increased threshold for smoother point transition
                self.x = nx
                self.y = ny
                # Don't zero out velocity here to maintain "water-like" momentum
                if self.path:
                    self.path.pop(0)
            else:
                # --- Smooth steering movement (dt-based) --- 
                dt = max(0.0, min(0.05, now - getattr(self, "_last_t", now))) 
                self._last_t = now 

                # Target is next waypoint 
                tx, ty = nx, ny 
                dx = tx - self.x 
                dy = ty - self.y 
                dist = math.sqrt(dx*dx + dy*dy) or 0.0001 

                # Arrive behavior: slow down near target 
                desired_speed = self.max_speed 
                if dist < self.slow_radius: 
                    desired_speed = self.max_speed * (dist / self.slow_radius) 
                    desired_speed = max(40.0, desired_speed) 

                desired_vx = (dx / dist) * desired_speed 
                desired_vy = (dy / dist) * desired_speed 

                # Blend collision avoidance gently (no big impulses) 
                avoid_vx, avoid_vy = self.compute_velocity_avoidance( 
                    other_robots, 
                    (desired_vx, desired_vy), 
                    time_horizon=2.0, 
                    radius=35 
                ) 

                # Steering = desired - current velocity (accel-limited) 
                steer_x = avoid_vx - self.vx 
                steer_y = avoid_vy - self.vy 

                steer_mag = math.sqrt(steer_x*steer_x + steer_y*steer_y) or 0.0001 
                max_steer = self.max_accel 
                if steer_mag > max_steer: 
                    scale = max_steer / steer_mag 
                    steer_x *= scale 
                    steer_y *= scale 

                # Apply accel 
                self.vx += steer_x * dt 
                self.vy += steer_y * dt 

                # Clamp speed 
                sp = math.sqrt(self.vx*self.vx + self.vy*self.vy) or 0.0001 
                if sp > self.max_speed: 
                    s = self.max_speed / sp 
                    self.vx *= s 
                    self.vy *= s 

                # Integrate position 
                self.x += self.vx * dt 
                self.y += self.vy * dt 

                # Consume waypoint if close enough 
                if dist <= self.arrive_radius: 
                    self.x = tx 
                    self.y = ty 
                    if self.path: 
                        self.path.pop(0) 

                # Battery drain scaled by dt 
                drain = self.battery_drain_rate * (dt * 60.0)  # normalize around ~60fps 
                if self.collision_avoidance_active: 
                    drain *= 1.2 
                self.battery = max(0, self.battery - drain)
                    
        elif self.status == "delivering":
            if now >= self.deliver_until:
                if self.current_parcel:
                    # Notify engine/callback
                    # We'll use a hack here to avoid circular imports or complex callbacks if possible
                    # but SimulationEngine will check robots in its step.
                    pass
                dx = DOCK_AREA["x"] + DOCK_AREA["w"] // 2
                dy = DOCK_AREA["y"] + DOCK_AREA["h"] // 2
                self.path = self.grid.astar((int(self.x), int(self.y)), (dx, dy))
                self.status = "returning"

    def to_dict(self):
        return {
            "id": self.id,
            "x": self.x,
            "y": self.y,
            "status": self.status,
            "zone": self.target_zone,
            "battery": self.battery,
            "path": self.path,
            "collision_avoidance_active": self.collision_avoidance_active,
            "type": self.type,
            "color": self.color
        }

class SimulationEngine:
    def __init__(self):
        self.grid = Grid()
        self.robots = []
        self.parcels_backlog = {z: deque() for z in ZONES.keys()}
        self.godowns = {z: {} for z in ZONES.keys()} # zone -> {city: count} (Phase 5)
        self.parcels_processed = 0
        self.last_update = time.time()
        self.forecast_volume = 1000
        self.collision_events = []
        self.on_parcel_delivered = None
        self.on_parcel_assigned = None
        self.on_robot_low_battery = None
        self.company_id = "demo_company"
        self.delivery_fleet = {} # vehicle_id -> { city, progress, status, load }

    def clear(self):
        """Resets the simulation state for a new manifest."""
        self.parcels_backlog = {z: deque() for z in ZONES.keys()}
        self.godowns = {z: {} for z in ZONES.keys()}
        self.parcels_processed = 0
        self.collision_events = []
        for r in self.robots:
            r.status = "idle"
            r.current_parcel = None
            r.path = []
            r.target_zone = None
            r.x = DOCK_AREA["x"] + 10
            r.y = DOCK_AREA["y"] + DOCK_AREA["h"] // 2

    def update_forecast(self, volume):
        self.forecast_volume = volume
        self.adjust_robot_count()

    def get_required_robots(self):
        if self.forecast_volume == 0:
            return 5 # Minimal fleet for standby
        elif self.forecast_volume < 800:
            return 5
        elif self.forecast_volume > 1500:
            return 20
        elif self.forecast_volume < 1000:
            return 8
        else:
            return 12

    def adjust_robot_count(self, target_count=None):
        if target_count is not None:
            target = target_count
        else:
            target = self.get_required_robots()

        # ONLY reset if the count has changed or we are initializing
        if len(self.robots) != target:
            self.robots = []
            for i in range(target):
                rid = f"R-{i + 1}"
                roll = random.random()
                if roll < 0.2: rtype = "Fast"
                elif roll < 0.4: rtype = "Heavy"
                else: rtype = "Standard"
                self.robots.append(Robot(rid, self.grid, rtype))
            
            # Reset all robots to dock position on fleet change
            for r in self.robots:
                r.x = DOCK_AREA["x"] + 10
                r.y = DOCK_AREA["y"] + DOCK_AREA["h"] // 2
                r.status = "idle"

    def pre_assign_robots(self, zone_distribution):
        """
        Pre-assigns robots to zones based on forecasted distribution.
        zone_distribution: dict {zone_name: robot_count}
        """
        robot_idx = 0
        for zone, count in zone_distribution.items():
            if zone not in ZONES: continue
            for _ in range(count):
                if robot_idx < len(self.robots):
                    robot = self.robots[robot_idx]
                    if robot.status == "idle":
                        # Pre-stage robot at the zone
                        robot.assign_task(zone)
                    robot_idx += 1

    def add_parcels(self, parcel_list):
        for p in parcel_list:
            zone = p.get("zone", "General")
            if zone not in ZONES:
                zone = "General"
            
            # Simple overflow check
            load = len(self.parcels_backlog[zone])
            cap = ZONE_CAPACITY.get(zone, 100)
            if load > int(cap * 0.3) and zone != "General":
                zone = "General"
            
            # Store full parcel data for assignment logic
            self.parcels_backlog[zone].append(p)

    def assign_tasks(self):
        priorities = ["High", "Medium", "Low"]
        
        # Sort idle robots to assign best fit first? 
        # Actually, we iterate robots.
        idle_robots = [r for r in self.robots if r.status == "idle"]
        
        # Group robots by type
        fast_robots = [r for r in idle_robots if r.type == "Fast"]
        heavy_robots = [r for r in idle_robots if r.type == "Heavy"]
        std_robots = [r for r in idle_robots if r.type == "Standard"]
        
        # Helper to find task
        def find_and_assign(robot_list, criteria_func=None):
            for robot in robot_list:
                if robot.status != "idle": continue
                
                chosen_zone = None
                chosen_parcel = None
                
                # Iterate priorities
                for pr in priorities:
                    for z, q in self.parcels_backlog.items():
                        if not q: continue
                        
                        # Peek at first parcel
                        p = q[0]
                        p_prio = p.get("priority", "Medium")
                        p_weight = float(p.get("weight_kg", 5.0))
                        
                        if p_prio == pr:
                            # Check criteria
                            if criteria_func and not criteria_func(p_prio, p_weight):
                                continue
                                
                            chosen_zone = z
                            chosen_parcel = p
                            break
                    if chosen_zone: break
                
                if chosen_zone:
                    self.parcels_backlog[chosen_zone].popleft()
                    robot.assign_task(chosen_zone, chosen_parcel)
                    if self.on_parcel_assigned and chosen_parcel:
                        self.on_parcel_assigned(chosen_parcel["id"], robot.id, self.company_id)
        
        # EFFICIENT ASSIGNMENT LOGIC
        
        # 1. Assign Heavy Robots (Strictly Prefer heavy items > 10kg)
        find_and_assign(heavy_robots, lambda pr, w: w > 10)
        
        # 2. Assign Fast Robots (Strictly Prefer High priority)
        find_and_assign(fast_robots, lambda pr, w: pr == "High")
        
        # 3. Cleanup: Assign remaining robots to ANY valid task
        # Heavy robots take remaining heavy or any? Let's say any if idle.
        find_and_assign(heavy_robots)
        # Fast robots take remaining
        find_and_assign(fast_robots)
        # Standard robots take anything
        find_and_assign(std_robots)


    def detect_collisions(self, now):
        """
        Passive collision detection for logging/stats only.
        Actual avoidance is handled by Robot.move_step using velocity obstacles.
        """
        n = len(self.robots)
        for i in range(n):
            for j in range(i + 1, n):
                r1 = self.robots[i]
                r2 = self.robots[j]
                dx = r1.x - r2.x
                dy = r1.y - r2.y
                d = math.sqrt(dx * dx + dy * dy)
                # If robots are physically overlapping (radius 10 each -> 20 diameter)
                if d < 20: 
                    self.collision_events.append((now, r1.id, r2.id))
                    if len(self.collision_events) > 50:
                        self.collision_events.pop(0)

    def update_delivery_fleet(self, dt):
        """
        Simulates movement of delivery vehicles from hub to destination cities.
        """
        # Cities to simulate
        cities = ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai", "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow"]
        
        # Ensure we have some vehicles if none exist
        if not self.delivery_fleet:
            for i in range(5):
                vid = f"V-{100 + i}"
                city = random.choice(cities)
                self.delivery_fleet[vid] = {
                    "id": vid,
                    "city": city,
                    "progress": random.random(), # 0.0 to 1.0
                    "status": "IN_TRANSIT",
                    "load": random.randint(50, 200),
                    "speed": random.uniform(0.01, 0.03) # Progress per step
                }

        # Update progress
        for vid, v in list(self.delivery_fleet.items()):
            v["progress"] += v["speed"] * dt * 10 # Speed scaled by time
            
            if v["progress"] >= 1.0:
                v["progress"] = 1.0
                v["status"] = "ARRIVED"
                # Wait a bit then reset to Hub
                if random.random() < 0.1: # 10% chance to reset each step after arrival
                    v["progress"] = 0.0
                    v["status"] = "IN_TRANSIT"
                    v["city"] = random.choice(cities)
                    v["load"] = random.randint(50, 200)

    def step(self):
        now = time.time()
        dt = max(0.0, min(0.1, now - self.last_update))
        self.last_update = now
        
        self.assign_tasks()
        
        # Check if we should trigger auto-dispatch (100 parcels sorted)
        if self.parcels_processed >= 100 and not getattr(self, "_dispatch_triggered", False):
            self._dispatch_triggered = True
            # We'll use a local import to avoid circular dependency
            try:
                from backend import event_bridge, agents
                event_bridge.emit("TRANSPORT_LOG", {
                    "type": "AUTO_DISPATCH",
                    "message": "Sorting complete for 100+ parcels. Fleet dispatch initiated."
                })
                agents["optimizer"].log(f"Auto-dispatch triggered: {self.parcels_processed} parcels sorted.", company_id=self.company_id)
            except Exception as e:
                print(f"Auto-dispatch trigger failed: {e}")

        for r in self.robots:
            prev_status = r.status
            r.move_step(now, self.robots)
            
            # Battery Monitoring
            if r.battery < r.low_battery_threshold and not r.notified_low_battery:
                if self.on_robot_low_battery:
                    self.on_robot_low_battery(r.id, r.battery, self.company_id)
                r.notified_low_battery = True
            
            if prev_status == "delivering" and r.status == "returning":
                if r.current_parcel and self.on_parcel_delivered:
                    # Update godown allocation (Phase 5)
                    parcel = r.current_parcel
                    zone = r.target_zone
                    city = parcel.get("destination_city", "Unknown")
                    if zone in self.godowns:
                        self.godowns[zone][city] = self.godowns[zone].get(city, 0) + 1
                    
                    self.on_parcel_delivered(parcel["id"], self.company_id)
                    r.current_parcel = None
                    self.parcels_processed += 1
        
        self.update_delivery_fleet(dt)
        self.detect_collisions(now)

    def get_state(self):
        zone_loads = {z: sum(self.godowns[z].values()) for z in ZONES.keys()}
        
        # Calculate Mock CO2 Efficiency
        # Standard: 1.0, Fast: 1.5 (less efficient), Heavy: 0.7 (more efficient per kg)
        total_co2_savings = 0.0
        for r in self.robots:
            if r.status != "idle":
                base_saving = 0.5
                multiplier = 1.2 if r.type == "Heavy" else (0.8 if r.type == "Fast" else 1.0)
                total_co2_savings += base_saving * multiplier

        return {
            "zones": ZONES,
            "dock": DOCK_AREA,
            "robots": [
                {
                    "id": r.id,
                    "x": r.x,
                    "y": r.y,
                    "status": r.status,
                    "zone": r.target_zone,
                    "parcel_id": r.current_parcel["id"] if r.current_parcel else None,
                    "battery": round(r.battery, 1),
                    "path": r.path,
                    "collision_avoidance_active": r.collision_avoidance_active,
                    "type": r.type,
                    "color": r.color
                }
                for r in self.robots
            ],
            "backlog": zone_loads,
            "godowns": self.godowns, # Phase 5
            "stats": {
                "parcels_processed": self.parcels_processed, 
                "zone_loads": zone_loads,
                "co2_savings_kg": round(total_co2_savings, 2)
            },
            "delivery_fleet": self.delivery_fleet
        }

sim_engine = SimulationEngine()
