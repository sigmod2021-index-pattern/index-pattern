package sigmod2021.db.core.secondaryindex;

import sigmod2021.db.event.TID;
import sigmod2021.event.Event;

import java.util.concurrent.Future;

public class EventAndTID {

    private Event event;
    private Future<TID> tid;

    public EventAndTID(Event event, Future<TID> tid) {
        this.event = event;
        this.tid = tid;
    }

    public Event getEvent() {
        return event;
    }

    public Future<TID> getTid() {
        return tid;
    }
}
