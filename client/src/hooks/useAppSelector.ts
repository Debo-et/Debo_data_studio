import { useSelector } from 'react-redux'
import type { RootState } from '../store'

// Use the new .withTypes() method
export const useAppSelector = useSelector.withTypes<RootState>()
