from mrjob.job import MRJob

class MRHotelRaitingCount(MRJob):
    # def mapper(self, _, line):
    #     (HName, HStar, HRooms, UCountry, NrReviews, rating, StayPeriod, TType, Pool, Gym, TCourt, Spa, Casino, Internet, UContinent, ReviewMonth, ReviewDay) = line.split("\t")
    #     result = [rating, 1]
    #     yield result

    # def reducer(self, key, value):
    #     result = [key, sum(value)]
    #     yield result


     def mapper(self, _, line):
         (userId, movieId, rating, timestamp) = line.split(",")
         result = [movieId, rating]
         if rating.isnumeric() == True:
            float(rating)            
            yield result

     def reducer(self, key, value):
         result = [key, sum(value)]
         yield result


if __name__ == '__main__':
    MRHotelRaitingCount.run()