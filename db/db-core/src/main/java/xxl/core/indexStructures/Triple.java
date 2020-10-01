package xxl.core.indexStructures;

import xxl.core.util.Pair;

public class Triple<E1,E2,E3> extends Pair<E1,E2> {

	private E3 third;
	
	public E3 getThird() {
		return third;
	}

	public void setThird(E3 third) {
		this.third = third;
	}

	public Triple(E1 first, E2 second, E3 third) {
		super(first,second);
		this.third = third;
	}
	
	public void setElement3(E3 third) {
		this.third = third;
	}
	
	public E3 getElement3() {
		return third;
	}

}
