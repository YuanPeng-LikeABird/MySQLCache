#include "MathUtils.h"
#include <cmath>

using namespace std;

const double EPSILON = 1e-12;

bool MathUtils::sameFloat(double v1, double v2)
{
	return fabs(v1 - v2) < EPSILON;
}

bool MathUtils::isInteger(double value)
{
	return fmod(value, 1) == 0.0;
}
