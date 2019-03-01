package de.tuberlin.dima.bdapro.model;

public class ClusterCenter extends Point{

        public int id;

        public ClusterCenter(int id, double[] data) {
            super(data);
            this.id = id;
        }

        public ClusterCenter(int id, Point p) {
            super(p.getFields());
            this.id = id;
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        @Override
        public String toString() {
            return id + " " + super.toString();
        }

    }


}
