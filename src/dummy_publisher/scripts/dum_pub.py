import rospy
from dummy_publisher.msg import Dummy
import signal

class GracefulInterruptHandler(object):

    def __init__(self, sig=signal.SIGINT):
        self.sig = sig

    def __enter__(self):

        self.interrupted = False
        self.released = False

        self.original_handler = signal.getsignal(self.sig)

        def handler(signum, frame):
            self.release()
            self.interrupted = True

        signal.signal(self.sig, handler)

        return self

    def __exit__(self, type, value, tb):
        self.release()

    def release(self):

        if self.released:
            return False

        signal.signal(self.sig, self.original_handler)

        self.released = True

        return True


def main():
    pub = rospy.Publisher('dummy_publisher', Dummy, queue_size=10)
    rospy.init_node("dummy_publisher")
    r = rospy.Rate(10)
    with GracefulInterruptHandler() as h1:
        while not h1.interrupted:
            print("Publishing")
            dummy = Dummy()
            dummy.header.stamp = rospy.Time.now()
            dummy.data.data = "dummy"
            dummy.tags = [3, 4, 5]

            pub.publish(dummy)
            r.sleep()


    pass


if __name__ == "__main__":
    main()